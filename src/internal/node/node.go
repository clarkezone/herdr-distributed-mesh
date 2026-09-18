package node

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"time"

	agentflowv1 "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/buildinfo"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/herdr"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/herdrsession"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/identity"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/projects"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/protocol"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/state"
	"github.com/clarkezone/herdr-distributed-mesh/src/internal/transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var capabilities = []string{"node.heartbeat.v1"}

type Options struct {
	// OnRegistered runs after the authenticated hello and initial heartbeat.
	OnRegistered    func(string)
	Name            string
	ManagedProjects *projects.Managed
	HerdrExecutable string
	SessionManager  SessionManager
	// EnableAgentControl requires a socket, journal, and fresh coordinator
	// verification before effects. Run installs the production peer verifier.
	EnableAgentControl bool
	// QueryAgent and ControlAgent inject executors for RunSession. Run always
	// uses the local Herdr adapter instead.
	QueryAgent                func(context.Context, herdr.Config, *agentflowv1.AgentQueryRequest) (*agentflowv1.AgentQueryResult, error)
	ControlAgent              func(context.Context, herdr.Config, *agentflowv1.AgentControl) (*agentflowv1.AgentControlResult, error)
	StartAgent                startAgentExecutor
	StopAgent                 stopAgentExecutor
	ResolveLifecycleWorkspace func(context.Context, herdr.Config, string, projects.Binding) (string, error)
	CommandJournalPath        string
	RequiredServerTag         string
	HeartbeatInterval         time.Duration
	HerdrSocket               string
	InstanceID                string
	ReconnectDelay            time.Duration
	ReconnectMaximum          time.Duration
	ServerAddress             string
	Transport                 transport.Config
	// WorkspacePolicy is upgrade-only migration input in production. Run exports
	// it to the coordinator; it never authorizes managed mutations. Low-level
	// session fixtures without ManagedProjects retain legacy replay contracts.
	WorkspacePolicy  *projects.Policy
	LegacyPolicyPath string
	// VerifyServerPeer refreshes authorization of the actual stream peer before
	// workspace/worktree/agent effects in RunSession. A nil verifier fails closed. Run always
	// overrides this callback with its production WhoIs and required-tag check.
	VerifyServerPeer func(context.Context, string) error
}

func Run(ctx context.Context, options Options) (err error) {
	return run(ctx, options, nil)
}

// RunWithNetwork preserves all role locks and peer verification while borrowing
// the managed network. Only its caller owns the network lifetime.
func RunWithNetwork(ctx context.Context, options Options, network transport.RuntimeNetwork) error {
	if network == nil {
		return errors.New("shared node network is required")
	}
	return run(ctx, options, network)
}

func run(ctx context.Context, options Options, network transport.RuntimeNetwork) (err error) {
	borrowed := network != nil
	guard, err := state.PrepareRoleState(ctx, options.Transport.RoleStateDir, "node")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, guard.Close()) }()
	if borrowed {
		if err := guard.DisableFullRoleBackup(); err != nil {
			return err
		}
	}
	instanceID, err := identity.LoadOrCreate(options.Transport.RoleStateDir)
	if err != nil {
		return err
	}
	if options.InstanceID != "" && options.InstanceID != instanceID {
		return errors.New("node instance identity differs from persistent role state")
	}
	options.InstanceID = instanceID
	if options.LegacyPolicyPath != "" {
		if options.WorkspacePolicy != nil {
			return errors.New("node accepts only one legacy project migration source")
		}
		options.WorkspacePolicy, err = projects.Load(options.LegacyPolicyPath, instanceID)
		if err != nil {
			return fmt.Errorf("load local workspace policy: %w", err)
		}
	}
	options.SessionManager = nil
	if options.HerdrExecutable != "" {
		options.SessionManager, err = herdrsession.New(herdrsession.Config{Executable: options.HerdrExecutable})
		if err != nil {
			return err
		}
	}
	if (options.HerdrSocket != "" || options.SessionManager != nil) && options.ManagedProjects == nil {
		options.ManagedProjects = projects.NewManaged(options.InstanceID)
	}
	if options.ReconnectMaximum <= 0 {
		options.ReconnectMaximum = time.Minute
	}
	journal, err := openJournal(ctx, options)
	if err != nil {
		return err
	}
	var commands commandJournal
	if journal != nil {
		commands = journal
		defer func() {
			if closeErr := journal.Close(); closeErr != nil {
				err = errors.Join(err, storageError("close", closeErr))
			}
		}()
	}
	if network == nil {
		network, err = transport.Start(ctx, options.Transport)
		if err != nil {
			return err
		}
		defer func() { err = errors.Join(err, network.Close()) }()
	}
	if !borrowed {
		if err := guard.Activate(); err != nil {
			return err
		}
	}
	// Always override any injected verifier in production with fresh WhoIs of
	// the actual stream peer, not the configured server address or DNS identity.
	options.VerifyServerPeer = workspacePeerTagVerifier(network.IdentifyPeer, options.RequiredServerTag)
	options.QueryAgent, options.ControlAgent = nil, nil
	options.StartAgent, options.StopAgent, options.ResolveLifecycleWorkspace = nil, nil, nil
	self := network.SelfStatus()
	if err := self.Validate(options.Transport.Tags, time.Now()); err != nil {
		return fmt.Errorf("validate node tsnet identity: %w", err)
	}
	log.Printf(
		"node tsnet identity stable_id=%s dns=%s tags=%v key_expiry=%s",
		self.StableID,
		self.DNSName,
		self.Tags,
		formatExpiry(self.KeyExpiry),
	)

	dial := network.DialGRPC
	if commands != nil {
		dial = func(target string) (*grpc.ClientConn, error) {
			return network.DialGRPCWithPeerTag(target, options.RequiredServerTag)
		}
	}
	connection, err := dial(options.ServerAddress)
	if err != nil {
		return err
	}
	defer connection.Close()

	client := agentflowv1.NewNodeControlClient(connection)
	attempt := 0
	for {
		registered, err := runSessionWithJournal(ctx, client, options, commands, nil)
		var journalFailure *journalError
		if errors.As(err, &journalFailure) {
			return err
		}
		if ctx.Err() != nil {
			return nil
		}
		if isPermanentSessionError(err) {
			return fmt.Errorf("node session rejected permanently: %w", err)
		}
		if registered {
			attempt = 0
		}
		delay := reconnectDelay(options.ReconnectDelay, options.ReconnectMaximum, attempt, rand.Float64())
		log.Printf("node session ended: %v; reconnecting in %s", err, delay)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
			attempt++
		}
	}
}

func isPermanentSessionError(err error) bool {
	var journalFailure *journalError
	if errors.As(err, &journalFailure) {
		return true
	}
	switch status.Code(err) {
	case codes.PermissionDenied, codes.Unauthenticated, codes.FailedPrecondition, codes.InvalidArgument:
		return true
	default:
		return false
	}
}

func runSession(ctx context.Context, client agentflowv1.NodeControlClient, options Options) (bool, error) {
	return RunSession(ctx, client, options)
}

func runSessionWithJournal(ctx context.Context, client agentflowv1.NodeControlClient, options Options, journal commandJournal, execute func()) (bool, error) {
	return runSessionWithExecutor(ctx, client, options, journal, execute, nil)
}

func runSessionWithExecutor(ctx context.Context, client agentflowv1.NodeControlClient, options Options, journal commandJournal, execute func(), ensure workspaceExecutor) (registered bool, sessionErr error) {
	return runSessionWithMutationExecutors(ctx, client, options, journal, execute, ensure, nil)
}

func runSessionWithMutationExecutors(ctx context.Context, client agentflowv1.NodeControlClient, options Options, journal commandJournal, execute func(), ensure workspaceExecutor, create worktreeExecutor) (registered bool, sessionErr error) {
	sessionContext, cancel := context.WithCancel(ctx)
	defer cancel()
	// Run retains its journal across reconnects. The previous session joins its
	// executor before returning, so abandoned intents are now safe to recover.
	if journal != nil {
		operation, stop := context.WithTimeout(sessionContext, journalOperationTimeout)
		err := journal.Recover(operation)
		stop()
		if err != nil {
			return false, storageError("recover", err)
		}
	}

	stream, err := client.Connect(sessionContext)
	if err != nil {
		return false, fmt.Errorf("open node control stream: %w", err)
	}
	// Cancel the entire stream on a blocked send; only this goroutine sends.
	send := func(envelope *agentflowv1.NodeEnvelope) error {
		timer := time.AfterFunc(10*time.Second, cancel)
		defer timer.Stop()
		return stream.Send(envelope)
	}
	advertised := slices.Clone(capabilities)
	_, lifecycleAvailable := journal.(lifecycleJournal)
	// Raw Windows pipes have no protocol-18 PID:start marker to pin.
	// Existing read/control support remains, but lifecycle must fail closed.
	lifecycleAvailable = lifecycleAvailable && (options.SessionManager != nil || !strings.HasPrefix(strings.ToLower(options.HerdrSocket), `\\.\pipe\`))
	projectCache, projectsEnabled := journal.(projectJournal)
	projectsEnabled = projectsEnabled && (options.HerdrSocket != "" || options.SessionManager != nil) && options.ManagedProjects != nil
	if options.HerdrSocket != "" {
		advertised = append(advertised, protocol.HerdrReadCapability)
	}
	if journal != nil {
		advertised = append(advertised, protocol.ProbeCapability)
		if options.SessionManager != nil {
			advertised = append(advertised, protocol.SessionManageCapability)
		}
		if options.EnableAgentControl && (options.HerdrSocket != "" || options.SessionManager != nil) {
			advertised = append(advertised, protocol.AgentControlCapability)
			if lifecycleAvailable {
				advertised = append(advertised, protocol.AgentLifecycleCapability)
			}
		}
		if projectsEnabled {
			advertised = append(advertised, protocol.ProjectConfigCapability, protocol.WorkspaceEnsureCapability, protocol.WorktreeCreateCapability)
		} else if options.WorkspacePolicy != nil && options.HerdrSocket != "" {
			advertised = append(advertised, protocol.WorkspaceEnsureCapability)
			if options.WorkspacePolicy.HasWorktrees() {
				advertised = append(advertised, protocol.WorktreeCreateCapability)
			}
		}
	}
	if err := send(&agentflowv1.NodeEnvelope{
		Body: &agentflowv1.NodeEnvelope_Hello{
			Hello: &agentflowv1.Hello{
				Protocol:              protocol.SupportedRange(),
				ImplementationVersion: buildinfo.Version,
				InstanceId:            options.InstanceID,
				Hostname:              options.Name,
				Role:                  agentflowv1.Role_ROLE_NODE,
				Capabilities:          advertised,
			},
		},
	}); err != nil {
		return false, fmt.Errorf("send hello: %w", err)
	}

	helloTimer := time.AfterFunc(10*time.Second, cancel)
	response, err := stream.Recv()
	helloTimer.Stop()
	if err != nil {
		return false, fmt.Errorf("receive hello acknowledgement: %w", err)
	}
	ack := response.GetHelloAck()
	if ack == nil {
		return false, errors.New("server did not respond with hello acknowledgement")
	}
	if ack.SelectedProtocol < protocol.MinimumVersion || ack.SelectedProtocol > protocol.MaximumVersion {
		return false, fmt.Errorf("server selected unsupported protocol %d", ack.SelectedProtocol)
	}
	if options.HerdrSocket != "" && !slices.Contains(ack.Capabilities, protocol.HerdrReadCapability) {
		return false, status.Error(codes.FailedPrecondition, "server does not support read-only Herdr state; upgrade server first")
	}
	commandReady := journal != nil && slices.Contains(ack.Capabilities, protocol.ProbeCapability)
	sessionsNegotiated := options.SessionManager != nil && commandReady && slices.Contains(ack.Capabilities, protocol.SessionManageCapability)
	if options.SessionManager != nil && !sessionsNegotiated {
		return false, status.Error(codes.FailedPrecondition, "server does not support named sessions; upgrade server first")
	}
	agentNegotiated := options.EnableAgentControl && (options.HerdrSocket != "" || options.SessionManager != nil) && commandReady && slices.Contains(ack.Capabilities, protocol.AgentControlCapability)
	lifecycleNegotiated := lifecycleAvailable && agentNegotiated && slices.Contains(ack.Capabilities, protocol.AgentLifecycleCapability)
	if options.EnableAgentControl && !agentNegotiated {
		return false, status.Error(codes.FailedPrecondition, "server does not support agent control; upgrade server first")
	}
	// Keep terminal replay available when a policy is removed. New claims still
	// require the local policy before any effect.
	workspaceNegotiated := journal != nil && slices.Contains(ack.Capabilities, protocol.WorkspaceEnsureCapability)
	worktreeNegotiated := journal != nil && slices.Contains(ack.Capabilities, protocol.WorktreeCreateCapability)
	if options.WorkspacePolicy != nil && !workspaceNegotiated {
		return false, status.Error(codes.FailedPrecondition, "server does not support workspace ensure; upgrade server first")
	}
	if options.WorkspacePolicy != nil && options.WorkspacePolicy.HasWorktrees() && !worktreeNegotiated {
		return false, status.Error(codes.FailedPrecondition, "server does not support worktree create; upgrade server first")
	}
	handler := &commandHandler{journal: journal, nodeID: options.InstanceID, execute: execute,
		workspacePolicy: options.WorkspacePolicy, workspaceNegotiated: workspaceNegotiated,
		worktreeNegotiated: worktreeNegotiated, createWorktree: create,
		agentNegotiated: agentNegotiated, controlAgent: options.ControlAgent,
		lifecycleNegotiated: lifecycleNegotiated, startAgent: options.StartAgent, stopAgent: options.StopAgent,
		resolveLifecycleWorkspace: options.ResolveLifecycleWorkspace,
		sessionsNegotiated:        sessionsNegotiated, sessions: options.SessionManager,
		herdrConfig: herdr.Config{SocketPath: options.HerdrSocket}, ensureWorkspace: ensure,
		verifyCoordinator: streamPeerVerifier(stream.Context(), options.VerifyServerPeer)}
	if options.VerifyServerPeer == nil {
		handler.verifyCoordinator = nil
	}
	if projectsEnabled {
		if !slices.Contains(ack.Capabilities, protocol.ProjectConfigCapability) {
			// Existing-agent controls remain available against an older server.
			// Legacy files must not become fallback authority in managed mode.
			projectsEnabled = false
			handler.workspacePolicy = nil
		}
	}
	if projectsEnabled {
		handler.managedProjects = projects.NewManaged(options.InstanceID)
		if err := handler.restoreProjects(sessionContext, projectCache, options.WorkspacePolicy); err != nil {
			return true, err
		}
		offer := &agentflowv1.LegacyProjects{}
		for _, binding := range options.WorkspacePolicy.LegacyBindings() {
			offer.Projects = append(offer.Projects, &agentflowv1.RegisterProjectRequest{
				NodeInstanceId: options.InstanceID, ProjectId: binding.ProjectID,
				CheckoutPath: binding.Path, WorktreeRoot: binding.WorktreeRoot})
		}
		if len(offer.Projects) > 0 {
			if err := send(&agentflowv1.NodeEnvelope{Body: &agentflowv1.NodeEnvelope_LegacyProjects{LegacyProjects: offer}}); err != nil {
				return true, err
			}
		}
	}
	queries := newAgentQueries(sessionContext, options, handler)
	defer func() { cancel(); queries.join() }()
	var pending []*agentflowv1.CommandResult
	if commandReady || workspaceNegotiated || worktreeNegotiated {
		operation, stop := context.WithTimeout(sessionContext, journalOperationTimeout)
		pending, err = journal.PendingResults(operation)
		stop()
		if err != nil {
			return true, storageError("pending results", err)
		}
	}
	log.Printf(
		"node registered instance_id=%s server_instance_id=%s protocol=%d",
		options.InstanceID,
		ack.ServerInstanceId,
		ack.SelectedProtocol,
	)

	type incomingMessage struct {
		envelope   *agentflowv1.NodeEnvelope
		receivedAt time.Time
		err        error
	}
	incoming := make(chan incomingMessage, 16)
	receiveDone := make(chan struct{})
	go func() {
		defer close(receiveDone)
		for {
			envelope, err := stream.Recv()
			select {
			case incoming <- incomingMessage{envelope: envelope, receivedAt: time.Now(), err: err}:
			case <-sessionContext.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	defer func() { cancel(); <-receiveDone; _ = stream.CloseSend() }()

	type commandWork struct {
		command    *agentflowv1.Command
		receivedAt time.Time
	}
	type commandOutcome struct {
		result *agentflowv1.CommandResult
		err    error
	}
	work := make(chan commandWork, 16)
	sessionWork := make(chan commandWork, 4)
	interruptWork := make(chan commandWork, 4)
	lifecycleWork := make(chan commandWork, 4)
	progress := make(chan *agentflowv1.CommandProgress, 16)
	handler.lifecycleProgress = progress
	outcomes := make(chan commandOutcome, 4)
	var workers sync.WaitGroup
	var workerErrors commandWorkerErrors
	// Long ensure and query waits cannot serialize an unrelated interrupt.
	runWorker := func(queue <-chan commandWork) {
		defer workers.Done()
		for {
			select {
			case <-sessionContext.Done():
				return
			case item := <-queue:
				result, err := handler.handle(sessionContext, item.command, item.receivedAt)
				if err != nil {
					workerErrors.Lock()
					workerErrors.errors = append(workerErrors.errors, err)
					workerErrors.Unlock()
				}
				select {
				case outcomes <- commandOutcome{result: result, err: err}:
				case <-sessionContext.Done():
					return
				}
				if err != nil {
					return
				}
			}
		}
	}
	for _, queue := range []<-chan commandWork{work, sessionWork, interruptWork, lifecycleWork} {
		workers.Add(1)
		go runWorker(queue)
	}
	defer func() {
		cancel()
		workers.Wait()
		for _, workerErr := range workerErrors.errors {
			var fatal *journalError
			if errors.As(workerErr, &fatal) {
				sessionErr = errors.Join(sessionErr, workerErr)
			}
		}
	}()

	updates := make(chan *agentflowv1.HerdrState, 1)
	sessionUpdates := make(chan *agentflowv1.SessionInventory, 1)
	observerErr := make(chan error, 2)
	if sessionsNegotiated {
		done := make(chan struct{})
		go func() {
			defer close(done)
			observerErr <- handler.observeSessions(sessionContext, func(inventory *agentflowv1.SessionInventory) error {
				select {
				case sessionUpdates <- inventory:
					return nil
				case <-sessionContext.Done():
					return sessionContext.Err()
				}
			})
		}()
		defer func() { cancel(); <-done }()
	}
	if options.HerdrSocket != "" {
		done := make(chan struct{})
		go func() {
			defer close(done)
			observerErr <- herdr.Observe(sessionContext, herdr.Config{SocketPath: options.HerdrSocket, ProjectResolver: handler.managedProjects},
				func(state *agentflowv1.HerdrState) error {
					select {
					case updates <- state:
						return nil
					case <-sessionContext.Done():
						return sessionContext.Err()
					}
				})
		}()
		defer func() { cancel(); <-done }()
	}

	heartbeatInterval := options.HeartbeatInterval
	if heartbeatInterval <= 0 {
		heartbeatInterval = 5 * time.Second
	}
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	replayTicker := time.NewTicker(5 * time.Millisecond)
	defer replayTicker.Stop()
	var replay <-chan time.Time
	if len(pending) > 0 {
		replay = replayTicker.C
	}
	var sequence uint64
	var localState *agentflowv1.HerdrState
	sendHeartbeat := func(timestamp time.Time) error {
		sequence++
		workspaceReady, worktreeReady := handler.mutationReadiness(localState, time.Now(), commandReady)
		return send(&agentflowv1.NodeEnvelope{
			Body: &agentflowv1.NodeEnvelope_Heartbeat{
				Heartbeat: &agentflowv1.Heartbeat{
					Sequence: sequence, SentAt: timestamppb.New(timestamp), CommandReady: commandReady,
					WorkspaceReady: workspaceReady,
					WorktreeReady:  worktreeReady,
					AgentReady:     handler.agentReady(),
					SessionsReady:  handler.sessionsReady(),
				},
			},
		})
	}
	if err := sendHeartbeat(time.Now()); err != nil {
		return true, fmt.Errorf("send initial heartbeat: %w", err)
	}
	if options.OnRegistered != nil {
		options.OnRegistered(options.InstanceID)
	}
	handleIncoming := func(message incomingMessage) error {
		if message.err != nil {
			if errors.Is(message.err, io.EOF) {
				return errors.New("server closed the node control stream")
			}
			return fmt.Errorf("receive server message: %w", message.err)
		}
		envelope := message.envelope
		if envelope == nil {
			return status.Error(codes.InvalidArgument, "empty server envelope")
		}
		if config := envelope.GetProjectConfig(); config != nil {
			if !projectsEnabled || len(envelope.ProtoReflect().GetUnknown()) != 0 {
				return status.Error(codes.FailedPrecondition, "project capability not negotiated")
			}
			ack, err := handler.applyProject(sessionContext, config)
			if err != nil {
				return err
			}
			if ack != nil {
				return send(&agentflowv1.NodeEnvelope{Body: &agentflowv1.NodeEnvelope_ProjectAck{ProjectAck: ack}})
			}
			return nil
		}
		if envelope.GetAgentQuery() != nil || envelope.GetAgentQueryCancel() != nil {
			if len(envelope.ProtoReflect().GetUnknown()) != 0 || !agentNegotiated {
				return status.Error(codes.FailedPrecondition, "agent query capability was not negotiated")
			}
			if query := envelope.GetAgentQuery(); query != nil {
				return queries.start(query)
			}
			return queries.cancelQuery(envelope.GetAgentQueryCancel())
		}
		if envelope.GetCommand() == nil && envelope.GetCommandAck() == nil {
			return nil
		}
		if !commandReady && !workspaceNegotiated && !worktreeNegotiated {
			return status.Error(codes.FailedPrecondition, "server sent command traffic without negotiated readiness")
		}
		if len(envelope.ProtoReflect().GetUnknown()) != 0 {
			return status.Error(codes.InvalidArgument, "unknown command envelope fields")
		}
		if command := envelope.GetCommand(); command != nil {
			if err := protocol.ValidateCommand(command, options.InstanceID); err != nil {
				return status.Errorf(codes.InvalidArgument, "invalid command: %v", err)
			}
			if (command.CommandType == protocol.ProbeCommandType && !commandReady) ||
				(command.CommandType == protocol.WorkspaceEnsureCommandType && !workspaceNegotiated) ||
				(command.CommandType == protocol.WorktreeCreateCommandType && !worktreeNegotiated) ||
				(command.CommandType == protocol.AgentControlCommandType && !agentNegotiated) ||
				(protocol.IsLifecycleCommand(command.CommandType) && !lifecycleNegotiated) ||
				(command.CommandType == protocol.SessionEnsureCommandType && !sessionsNegotiated) {
				return status.Error(codes.FailedPrecondition, "command capability was not negotiated")
			}
			queue := work
			if command.SessionEnsure != nil {
				queue = sessionWork
			} else if command.AgentStart != nil {
				queue = lifecycleWork
			} else if command.AgentStop != nil || command.AgentControl.GetAction() == agentflowv1.AgentControlAction_AGENT_CONTROL_ACTION_INTERRUPT {
				queue = interruptWork
			}
			select {
			case queue <- commandWork{command: command, receivedAt: message.receivedAt}:
				return nil
			default:
				return status.Error(codes.ResourceExhausted, "command queue full; reconnect required")
			}
		}
		return handler.acknowledge(sessionContext, envelope.GetCommandAck())
	}
	sendOutcome := func(outcome commandOutcome) error {
		if outcome.err != nil {
			return outcome.err
		}
		return send(&agentflowv1.NodeEnvelope{Body: &agentflowv1.NodeEnvelope_CommandResult{CommandResult: outcome.result}})
	}
	for {
		// Prioritize due heartbeats before each bounded unit of command/replay work.
		select {
		case timestamp := <-ticker.C:
			if err := sendHeartbeat(timestamp); err != nil {
				return true, fmt.Errorf("send heartbeat: %w", err)
			}
		default:
		}
		select {
		case <-sessionContext.Done():
			return true, sessionContext.Err()
		case result := <-queries.results:
			if err := send(&agentflowv1.NodeEnvelope{Body: &agentflowv1.NodeEnvelope_AgentQueryResult{AgentQueryResult: result}}); err != nil {
				return true, err
			}
		case outcome := <-outcomes:
			if err := sendOutcome(outcome); err != nil {
				return true, err
			}
		case checkpoint := <-progress:
			if err := send(&agentflowv1.NodeEnvelope{Body: &agentflowv1.NodeEnvelope_CommandProgress{CommandProgress: checkpoint}}); err != nil {
				return true, err
			}
		case err := <-observerErr:
			if err == nil {
				err = errors.New("Herdr observer stopped unexpectedly")
			}
			return true, fmt.Errorf("observe Herdr: %w", err)
		case state := <-updates:
			localState = state
			handler.agentBaseline.Store(state)
			if err := send(&agentflowv1.NodeEnvelope{Body: &agentflowv1.NodeEnvelope_HerdrState{HerdrState: state}}); err != nil {
				return true, fmt.Errorf("send Herdr state: %w", err)
			}
		case inventory := <-sessionUpdates:
			if err := send(&agentflowv1.NodeEnvelope{Body: &agentflowv1.NodeEnvelope_SessionInventory{SessionInventory: inventory}}); err != nil {
				return true, fmt.Errorf("send session inventory: %w", err)
			}
		case message := <-incoming:
			if err := handleIncoming(message); err != nil {
				return true, err
			}
		case <-replay:
			// Drain one incoming acknowledgement before replay, never the whole queue.
			select {
			case message := <-incoming:
				if err := handleIncoming(message); err != nil {
					return true, err
				}
			default:
			}
			result := pending[0]
			pending[0] = nil
			pending = pending[1:]
			if err := protocol.ValidateCommandResult(result); err != nil {
				return true, storageError("pending result", err)
			}
			if err := send(&agentflowv1.NodeEnvelope{Body: &agentflowv1.NodeEnvelope_CommandResult{CommandResult: result}}); err != nil {
				return true, fmt.Errorf("replay command result: %w", err)
			}
			if len(pending) == 0 {
				replay = nil
			}
		case timestamp := <-ticker.C:
			if err := sendHeartbeat(timestamp); err != nil {
				return true, fmt.Errorf("send heartbeat: %w", err)
			}
		}
	}
}

func (h *commandHandler) mutationReadiness(state *agentflowv1.HerdrState, now time.Time, commandReady bool) (workspace, worktree bool) {
	if h.managedProjects != nil {
		workspace = h.workspaceNegotiated && commandReady && workspaceBaselineReady(state, now)
		return workspace, workspace && h.worktreeNegotiated
	}
	workspace = h.workspaceNegotiated && h.workspacePolicy != nil && workspaceBaselineReady(state, now)
	worktree = h.worktreeNegotiated && commandReady && workspace && h.workspacePolicy.HasWorktrees()
	return workspace, worktree
}

func workspaceBaselineReady(state *agentflowv1.HerdrState, now time.Time) bool {
	if state == nil || state.Status != "ready" || state.ObservedAt == nil || state.ObservedAt.CheckValid() != nil {
		return false
	}
	age := now.Sub(state.ObservedAt.AsTime())
	return age >= 0 && age <= 30*time.Second
}

func reconnectDelay(base, maximum time.Duration, attempt int, jitter float64) time.Duration {
	delay := base
	for range attempt {
		if delay >= maximum/2 {
			delay = maximum
			break
		}
		delay *= 2
	}
	if delay > maximum {
		delay = maximum
	}
	factor := 0.8 + 0.4*jitter
	jittered := time.Duration(float64(delay) * factor)
	if jittered > maximum {
		return maximum
	}
	return jittered
}

func formatExpiry(expiry *time.Time) string {
	if expiry == nil {
		return "none"
	}
	return expiry.Format(time.RFC3339)
}
