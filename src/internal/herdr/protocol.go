package herdr

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"regexp"
	"strings"
	"sync"
	"unicode/utf8"

	agentflowv1 "github.com/clarkezone/herdr-distributed-mesh/src/gen/agentflow/v1"
	"google.golang.org/protobuf/proto"
)

const (
	supportedProtocol = 18
	maxFrameBytes     = 2 * 1024 * 1024
	maxEntities       = 4096
	maxStateBytes     = 256 * 1024
)

type apiError string

func (e apiError) Error() string { return string(e) }

func category(err error) string {
	var api apiError
	if errors.As(err, &api) {
		return string(api)
	}
	return "io_error"
}

func ioFailure(err error) error {
	var timeout net.Error
	if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &timeout) && timeout.Timeout()) {
		return apiError("request_timeout")
	}
	if errors.Is(err, io.EOF) {
		return apiError("connection_closed")
	}
	return apiError("io_error")
}

type subscription struct {
	Type string `json:"type"`
}

func topologySubscriptions() []subscription {
	return []subscription{
		{"workspace.created"}, {"workspace.updated"}, {"workspace.closed"},
		{"workspace.renamed"}, {"workspace.focused"},
		{"tab.created"}, {"tab.closed"}, {"tab.renamed"}, {"tab.focused"},
		{"pane.created"}, {"pane.updated"}, {"pane.closed"}, {"pane.focused"},
		{"pane.agent_detected"}, {"layout.updated"},
	}
}

type object map[string]json.RawMessage

func validateEvent(event object) error {
	var kind string
	var data object
	if len(event) != 2 || required(event, "event", &kind) != nil || required(event, "data", &data) != nil {
		return apiError("invalid_response")
	}
	for _, sub := range topologySubscriptions() {
		// Protocol 18 uses dotted subscription selectors but snake_case
		// discriminators in emitted event envelopes.
		if kind == strings.ReplaceAll(sub.Type, ".", "_") {
			return nil
		}
	}
	return apiError("invalid_response")
}

// request leaves the acknowledged connection open. Its deadline bounds both
// write and read; cancellation closes it even on transports with blocked I/O.
func (o *observer) request(ctx context.Context, method, resultType string, params any) (net.Conn, *frameReader, func(), object, error) {
	requestCtx, cancel := context.WithTimeout(ctx, o.config.RequestTimeout)
	defer cancel()
	conn, err := o.dial(requestCtx, o.config.SocketPath)
	if err != nil {
		if requestCtx.Err() != nil {
			return nil, nil, nil, nil, ioFailure(requestCtx.Err())
		}
		return nil, nil, nil, nil, apiError("connection_failed")
	}
	closed := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		conn.Close()
		close(closed)
	})
	var once sync.Once
	cleanup := func() {
		once.Do(func() {
			if !stop() {
				<-closed
			}
			conn.Close()
		})
	}
	success := false
	defer func() {
		if !success {
			cleanup()
		}
	}()
	deadline, _ := requestCtx.Deadline()
	if err := conn.SetDeadline(deadline); err != nil {
		return nil, nil, nil, nil, apiError("io_error")
	}
	id := o.requestID()
	if err := json.NewEncoder(conn).Encode(struct {
		ID     string `json:"id"`
		Method string `json:"method"`
		Params any    `json:"params"`
	}{id, method, params}); err != nil {
		return nil, nil, nil, nil, ioFailure(err)
	}
	reader := &frameReader{bufio.NewReader(conn)}
	envelope, err := reader.object()
	if err != nil {
		return nil, nil, nil, nil, err
	}
	var responseID string
	if required(envelope, "id", &responseID) != nil || responseID != id || len(envelope) != 2 {
		return nil, nil, nil, nil, apiError("invalid_response")
	}
	if raw, ok := envelope["error"]; ok {
		var api object
		if decodeRequired(raw, &api) != nil {
			return nil, nil, nil, nil, apiError("invalid_response")
		}
		return nil, nil, nil, nil, apiError("api_error")
	}
	var result object
	var kind string
	if required(envelope, "result", &result) != nil || required(result, "type", &kind) != nil || kind != resultType {
		return nil, nil, nil, nil, apiError("invalid_response")
	}
	success = true
	return conn, reader, cleanup, result, nil
}

func (o *observer) rpc(ctx context.Context, method, resultType string) (object, error) {
	_, _, cleanup, result, err := o.request(ctx, method, resultType, struct{}{})
	if err != nil {
		return nil, err
	}
	cleanup()
	return result, nil
}

type frameReader struct{ reader *bufio.Reader }

func (r *frameReader) object() (object, error) {
	var frame []byte
	for {
		part, err := r.reader.ReadSlice('\n')
		if len(frame)+len(part) > maxFrameBytes {
			return nil, apiError("frame_too_large")
		}
		frame = append(frame, part...)
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if err != nil {
			if errors.Is(err, io.EOF) && len(frame) != 0 {
				return nil, apiError("malformed_json")
			}
			return nil, ioFailure(err)
		}
		break
	}
	if !utf8.Valid(frame) {
		return nil, apiError("malformed_json")
	}
	decoder := json.NewDecoder(bytes.NewReader(frame))
	decoder.UseNumber()
	if err := checkJSON(decoder, 0); err != nil {
		return nil, apiError("malformed_json")
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, apiError("malformed_json")
	}
	var value object
	if decodeRequired(frame, &value) != nil {
		return nil, apiError("invalid_response")
	}
	return value, nil
}

// Reject duplicate keys and excessive nesting rather than accepting ambiguous
// envelopes. Unknown snapshot fields are allowed, but never copied to output.
func checkJSON(d *json.Decoder, depth int) error {
	if depth > 64 {
		return apiError("malformed_json")
	}
	token, err := d.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		keys := make(map[string]struct{})
		for d.More() {
			key, err := d.Token()
			if err != nil {
				return err
			}
			name, ok := key.(string)
			if !ok {
				return apiError("malformed_json")
			}
			if _, exists := keys[name]; exists {
				return apiError("malformed_json")
			}
			keys[name] = struct{}{}
			if err := checkJSON(d, depth+1); err != nil {
				return err
			}
		}
	case '[':
		for d.More() {
			if err := checkJSON(d, depth+1); err != nil {
				return err
			}
		}
	default:
		return apiError("malformed_json")
	}
	_, err = d.Token()
	return err
}

func decodeRequired(raw json.RawMessage, into any) error {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return apiError("invalid_snapshot")
	}
	if err := json.Unmarshal(raw, into); err != nil {
		return apiError("invalid_snapshot")
	}
	return nil
}

func required(value object, key string, into any) error {
	return decodeRequired(value[key], into)
}

var (
	idPattern      = regexp.MustCompile(`^[A-Za-z0-9:_-]{1,128}$`)
	versionPattern = regexp.MustCompile(`^v?[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z][0-9A-Za-z.-]*)?(?:\+[0-9A-Za-z][0-9A-Za-z.-]*)?$`)
)

func versionProtocol(value object) (string, uint32, error) {
	var version string
	var protocol uint32
	if required(value, "version", &version) != nil || len(version) > 128 || !versionPattern.MatchString(version) ||
		required(value, "protocol", &protocol) != nil || protocol == 0 {
		return "", 0, apiError("invalid_snapshot")
	}
	return version, protocol, nil
}

func sanitizeSnapshot(raw json.RawMessage) (*agentflowv1.HerdrState, error) {
	return sanitizeSnapshotWithProjects(raw, nil)
}

func sanitizeSnapshotWithProjects(raw json.RawMessage, resolver ProjectResolver) (*agentflowv1.HerdrState, error) {
	var snapshot object
	if err := decodeRequired(raw, &snapshot); err != nil {
		return nil, err
	}
	version, protocol, err := versionProtocol(snapshot)
	if err != nil {
		return nil, err
	}
	if protocol != supportedProtocol {
		return nil, apiError("unsupported_protocol")
	}
	state := &agentflowv1.HerdrState{Status: "ready", Version: version, Protocol: protocol}
	for _, name := range []string{"focused_workspace_id", "focused_tab_id", "focused_pane_id"} {
		if raw, ok := snapshot[name]; ok && !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			var id string
			if json.Unmarshal(raw, &id) != nil || !idPattern.MatchString(id) {
				return nil, apiError("invalid_snapshot")
			}
		}
	}
	total := 0
	projectCache := make(map[string]projectObservation)
	projectFailed := false
	for _, collection := range []struct {
		name string
		id   string
		out  *[]*agentflowv1.HerdrEntity
	}{
		{"workspaces", "workspace_id", &state.Workspaces},
		{"tabs", "tab_id", &state.Tabs},
		{"panes", "pane_id", &state.Panes},
		{"agents", "pane_id", &state.Agents},
		{"layouts", "", nil},
	} {
		var entries []json.RawMessage
		if required(snapshot, collection.name, &entries) != nil {
			return nil, apiError("invalid_snapshot")
		}
		total += len(entries)
		if total > maxEntities {
			return nil, apiError("too_many_entities")
		}
		seen := make(map[string]struct{}, len(entries))
		for _, rawEntry := range entries {
			var entry object
			if decodeRequired(rawEntry, &entry) != nil {
				return nil, apiError("invalid_snapshot")
			}
			if collection.out == nil {
				continue
			}
			entity, err := sanitizeEntity(entry, collection.id)
			if err != nil {
				return nil, err
			}
			if _, duplicate := seen[entity.Id]; duplicate {
				return nil, apiError("invalid_snapshot")
			}
			seen[entity.Id] = struct{}{}
			if collection.name == "workspaces" && resolver != nil {
				projectID, err := workspaceProject(entry, resolver, projectCache)
				if err != nil {
					projectFailed = true
				} else {
					entity.ProjectId = projectID
				}
			}
			*collection.out = append(*collection.out, entity)
		}
	}
	inheritWorkspaceProjects(state)
	if projectFailed {
		log.Printf("herdr project projection unavailable")
	}
	return state, nil
}

func checkStateSize(state *agentflowv1.HerdrState) error {
	if proto.Size(state) > maxStateBytes {
		return apiError("state_too_large")
	}
	return nil
}

func sanitizeEntity(entry object, idField string) (*agentflowv1.HerdrEntity, error) {
	entity := &agentflowv1.HerdrEntity{}
	if required(entry, idField, &entity.Id) != nil || !idPattern.MatchString(entity.Id) ||
		required(entry, "focused", &entity.Focused) != nil ||
		required(entry, "agent_status", &entity.AgentStatus) != nil {
		return nil, apiError("invalid_snapshot")
	}
	if idField != "workspace_id" {
		if required(entry, "workspace_id", &entity.WorkspaceId) != nil || !idPattern.MatchString(entity.WorkspaceId) {
			return nil, apiError("invalid_snapshot")
		}
	}
	if idField == "pane_id" {
		if required(entry, "tab_id", &entity.TabId) != nil || !idPattern.MatchString(entity.TabId) {
			return nil, apiError("invalid_snapshot")
		}
		observation, err := readEntityObservation(entry)
		if err != nil {
			return nil, err
		}
		entity.Provider = observation.provider
		entity.InteractiveReady = observation.interactiveReady
		entity.TerminalId = observation.terminalID
		entity.ProviderSessionId = observation.agentSessionID
	}
	switch entity.AgentStatus {
	case "idle", "working", "blocked", "done", "unknown":
	default:
		entity.AgentStatus = "unknown"
	}
	return entity, nil
}
