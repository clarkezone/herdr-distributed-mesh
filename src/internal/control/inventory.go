package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	pb "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type InventoryFilter struct {
	NodeID, ProjectID, WorkspaceID, Provider, Readiness string
}

type inventorySource struct {
	NodeID              string     `json:"node_instance_id"`
	SessionName         string     `json:"session_name"`
	SessionIncarnation  string     `json:"session_incarnation"`
	Connected           bool       `json:"connected"`
	Stale               bool       `json:"stale"`
	Status              string     `json:"status"`
	ErrorCode           string     `json:"error_code,omitempty"`
	SessionManagerReady bool       `json:"session_manager_ready"`
	SessionManagerError string     `json:"session_manager_error_code,omitempty"`
	ObservedAt          *time.Time `json:"observed_at,omitempty"`
	ReceivedAt          *time.Time `json:"received_at,omitempty"`
}

type inventoryAgent struct {
	Source           inventorySource `json:"source"`
	Target           *pb.AgentTarget `json:"target"`
	WorkspaceID      string          `json:"workspace_id"`
	TabID            string          `json:"tab_id"`
	ProjectID        string          `json:"project_id,omitempty"`
	Provider         string          `json:"provider,omitempty"`
	InteractiveReady *bool           `json:"interactive_ready,omitempty"`
	ObservedStatus   string          `json:"observed_status"`
}

type agentInventory struct {
	Agents  []inventoryAgent  `json:"agents"`
	Sources []inventorySource `json:"sources"`
}

// AgentInventory reads the coordinator's redacted observed inventory, not live
// provider output. Sources remain visible even when no agents match a filter.
func AgentInventory(ctx context.Context, options Options, filter InventoryFilter) error {
	switch filter.Readiness {
	case "", "any", "true", "false", "unknown":
	default:
		return errors.New("agent inventory readiness must be any, true, false, or unknown")
	}
	if options.Output == nil {
		return errors.New("agent inventory requires an output writer")
	}
	return WithFleet(ctx, options, func(client pb.FleetClient) error {
		list, err := retryUnavailable(ctx, func() (*pb.NodeList, error) {
			return client.ListNodes(ctx, &emptypb.Empty{})
		})
		if err != nil {
			return fmt.Errorf("read agent inventory: %w", err)
		}
		if list == nil {
			return errors.New("agent inventory returned no fleet snapshot")
		}
		result := projectAgentInventory(list, filter)
		if options.JSON {
			return json.NewEncoder(options.Output).Encode(result)
		}
		for _, source := range result.Sources {
			var view humanView
			view.field("Source node", source.NodeID)
			if source.SessionName == "" {
				view.field("Session", "configured default")
			} else {
				view.session(source.SessionName, source.SessionIncarnation)
			}
			connection := "connected"
			if !source.Connected {
				connection = "disconnected; restore the node connection before acting"
			}
			view.field("Connection", connection)
			view.freshness(source.Stale)
			view.field("Herdr status", readable(source.Status))
			view.field("Issue", HumanDetail(source.ErrorCode))
			manager := "ready"
			if !source.SessionManagerReady {
				manager = "unavailable; inspect the execution node's managed-session configuration"
			}
			view.field("Session manager", manager)
			view.field("Session manager issue", HumanDetail(source.SessionManagerError))
			if _, err := fmt.Fprintln(options.Output, view.String()); err != nil {
				return err
			}
		}
		for _, agent := range result.Agents {
			readiness := "unknown; query the agent before sending input"
			if agent.InteractiveReady != nil {
				readiness = readyText(*agent.InteractiveReady)
			}
			var view humanView
			view.target(agent.Target)
			view.field("Target node", agent.Source.NodeID)
			view.field("Workspace", agent.WorkspaceID)
			view.field("Tab", agent.TabID)
			view.field("Project", agent.ProjectID)
			view.field("Provider", agent.Provider)
			view.field("Reported input readiness", readiness)
			view.field("Observed status", readable(agent.ObservedStatus))
			view.freshness(agent.Source.Stale)
			if _, err := fmt.Fprintln(options.Output, view.String()); err != nil {
				return err
			}
		}
		if len(result.Agents) == 0 {
			_, err := fmt.Fprintln(options.Output, "no observed agents match; inspect source availability above")
			return err
		}
		return nil
	})
}

func projectAgentInventory(list *pb.NodeList, filter InventoryFilter) agentInventory {
	result := agentInventory{Agents: []inventoryAgent{}, Sources: []inventorySource{}}
	add := func(source inventorySource, state *pb.HerdrState) {
		result.Sources = append(result.Sources, source)
		for _, agent := range state.GetAgents() {
			if (filter.ProjectID != "" && agent.ProjectId != filter.ProjectID) ||
				(filter.WorkspaceID != "" && agent.WorkspaceId != filter.WorkspaceID) ||
				(filter.Provider != "" && agent.Provider != filter.Provider) ||
				!inventoryReadyMatches(agent.InteractiveReady, filter.Readiness) {
				continue
			}
			result.Agents = append(result.Agents, inventoryAgent{
				Source: source, Target: &pb.AgentTarget{
					PaneId: agent.Id, TerminalId: agent.TerminalId, AgentSessionId: agent.ProviderSessionId,
					SessionName: source.SessionName, SessionIncarnation: source.SessionIncarnation,
				},
				WorkspaceID: agent.WorkspaceId, TabID: agent.TabId, ProjectID: agent.ProjectId,
				Provider: agent.Provider, InteractiveReady: agent.InteractiveReady, ObservedStatus: agent.AgentStatus,
			})
		}
	}
	for _, node := range list.Nodes {
		if filter.NodeID != "" && node.InstanceId != filter.NodeID {
			continue
		}
		source := makeInventorySource(node, "", "", node.Herdr, node.HerdrReceivedAt, node.Stale)
		add(source, node.Herdr)
		for _, session := range node.Sessions {
			source := makeInventorySource(node, session.Name, session.Incarnation, session.Herdr,
				session.HerdrReceivedAt, session.Stale || !node.SessionsReady || session.Status != "ready")
			if session.ErrorCode != "" {
				source.ErrorCode = session.ErrorCode
			} else if node.SessionsErrorCode != "" {
				source.ErrorCode = node.SessionsErrorCode
			}
			add(source, session.Herdr)
		}
	}
	lessSource := func(a, b inventorySource) bool {
		if a.NodeID != b.NodeID {
			return a.NodeID < b.NodeID
		}
		if a.SessionName != b.SessionName {
			return a.SessionName < b.SessionName
		}
		return a.SessionIncarnation < b.SessionIncarnation
	}
	sort.Slice(result.Sources, func(i, j int) bool { return lessSource(result.Sources[i], result.Sources[j]) })
	sort.Slice(result.Agents, func(i, j int) bool {
		a, b := result.Agents[i], result.Agents[j]
		if a.Source.NodeID != b.Source.NodeID || a.Source.SessionName != b.Source.SessionName ||
			a.Source.SessionIncarnation != b.Source.SessionIncarnation {
			return lessSource(a.Source, b.Source)
		}
		return a.Target.PaneId < b.Target.PaneId
	})
	return result
}

func makeInventorySource(node *pb.NodeView, name, incarnation string, state *pb.HerdrState, received *timestamppb.Timestamp, stale bool) inventorySource {
	source := inventorySource{
		NodeID: node.InstanceId, SessionName: name, SessionIncarnation: incarnation,
		Connected: node.Connected, Status: state.GetStatus(), ErrorCode: state.GetErrorCode(),
		Stale:               stale || !node.Connected || state.GetStatus() != "ready",
		SessionManagerReady: node.SessionsReady, SessionManagerError: node.SessionsErrorCode,
	}
	if received != nil && received.CheckValid() == nil {
		value := received.AsTime()
		source.ReceivedAt = &value
	} else {
		source.Stale = true
	}
	if observed := state.GetObservedAt(); observed != nil && observed.CheckValid() == nil {
		value := observed.AsTime()
		source.ObservedAt = &value
	}
	if source.Status == "" {
		source.Status = "unknown"
	}
	return source
}

func inventoryReadyMatches(ready *bool, filter string) bool {
	switch filter {
	case "true":
		return ready != nil && *ready
	case "false":
		return ready != nil && !*ready
	case "unknown":
		return ready == nil
	default:
		return true
	}
}
