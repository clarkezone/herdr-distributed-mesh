# Mesh MCP adapter

This package uses the official `github.com/modelcontextprotocol/go-sdk` v1.8.0
for MCP initialization, discovery, tool calls, validation, cancellation, and
stdio framing. `app\mcp.go` provides shared-service application wiring, including
named-session listing, ensure, pinned selection, and all 20 registered operations.
Start/stop mapping is tested against injected services; completion of B3's
node/server end-to-end validation remains a separate integration responsibility.
The adapter itself does not
connect to a mesh, run Herdr, execute subprocesses, read local configuration,
store transcripts, or independently implement an application operation.

## Entry points

```go
type InvokeFunc func(context.Context, Operation, any) (any, error)

func New(invoke InvokeFunc, options Options) (*mcp.Server, error)
func Run(ctx context.Context, invoke InvokeFunc, options Options) error
```

`New` creates the fixed 20-tool registry and permits SDK transport injection.
`Run` creates that server and runs the SDK's `StdioTransport`. The embedding
application must reserve stdout for MCP and send diagnostics to stderr.
There are no sampling, roots, resource subscriptions, automatic approvals,
input-required continuations, recursive tool dispatch, or follow tools.
Only explicit tool calls return application output.

`InvokeFunc` receives one of the exported `Operation` constants and a **concrete
input value, not a pointer**. Use a fixed switch to call the same application
services as the CLI; do not turn the callback into a raw API or shell dispatcher.
An unsupported operation must return an explicit error, never `nil, nil`.

## Input contract and wiring map

These are deliberately local types, independent of evolving protobuf types.
Every schema is an object that rejects unknown fields, including nested targets.
All IDs are nonempty bounded strings without whitespace/control characters.

| Tool | Concrete input |
| --- | --- |
| `list_nodes` | `PageInput` |
| `get_node` | `NodeInput` |
| `list_projects` | `ListProjectsInput` |
| `get_project` | `ProjectInput` |
| `register_project` | `RegisterProjectInput` |
| `list_sessions` | `ListSessionsInput` |
| `ensure_herdr_session` | `EnsureHerdrSessionInput` |
| `list_workspaces` | `ListWorkspacesInput` |
| `list_agents` | `ListAgentsInput` |
| `get_agent` | `AgentInput` |
| `read_agent` | `ReadAgentInput` |
| `wait_agent` | `WaitAgentInput` |
| `ensure_workspace` | `EnsureWorkspaceInput` |
| `create_worktree` | `CreateWorktreeInput` |
| `start_agent` | `StartAgentInput` |
| `prompt_agent` | `PromptAgentInput` |
| `send_agent_input` | `SendAgentInput` (`SendAgentInputOp` constant) |
| `interrupt_agent` | `MutateAgentInput` |
| `stop_agent` | `StopAgentInput` |
| `get_command` | `GetCommandInput` |

Ready tools use `node_instance_id` and, where backed by a service, `project_id`
or `workspace_id`. Existing-agent operations other than lifecycle stop require all of `pane_id`,
`terminal_id`, and `agent_session_id`; the latter pins the agent incarnation.
They do not accept unverifiable project associations. Existing-agent queries and
mutations, workspace/worktree mutations, and workspace/agent listings accept
optional **paired** `herdr_session_id` and `herdr_session_incarnation` selectors.
The former is the portable session name; the latter is its exact observed
64-character lowercase hexadecimal incarnation. Both must be supplied together
except for the pinned configured-endpoint stop described below.
Omitting both retains the configured-default behavior; supplying a name never
permits discovery, replacement, or fallback to another/default session.

Workspace mutations also require the project and `binding_revision`.
`list_workspaces` selects the default or exact named snapshot. Neither entity list
invents missing project, provider, or terminal data. Named workspace projections retain the selected
session's status, error code, receipt timestamp, and stale flag rather than the
default session's metadata. Their cursors are scoped to the selected incarnation.
Entity list projections omit unreported empty `provider`, `terminal_id`,
`provider_session_id`, and `project_id` strings emitted by ProtoJSON. Populated
metadata is preserved, as is explicit `interactive_ready: false` or `true`;
unknown readiness remains absent. Raw node results keep the ordinary RPC JSON shape.
Application protobuf results retain their native `session_name` and
`session_incarnation` fields without renaming them to the MCP input aliases.

`list_agents` now calls `control.AgentInventory`, the same observed-inventory
service as `ctl agents`, through the shared injected fleet client. Its optional
`project_id`, `workspace_id`, `provider`, and `readiness` filters use that service's
exact matching, not a parallel MCP implementation. Readiness accepts omitted,
empty, `any`, `true`, `false`, or `unknown`; unknown is not false. There are no
live native/provider reads or additional RPCs.

Agent-list output uses the shared inventory contract rather than the earlier
flat entity projection: `{agents: [{source, target, workspace_id, tab_id,
project_id?, provider?, interactive_ready?, observed_status}], sources,
next_cursor}`. Each target preserves the observed `AgentTarget` fields, without
guessing default session names, native incarnations, or missing terminal/provider
identities. The existing agent selection remains configured-default when both
session selectors are omitted, or exact named/incarnation-pinned otherwise.
Missing/replaced explicit scopes fail rather than falling back. All node
`sources` are retained on every page, including other sessions and empty or
unavailable scopes, even when no agent matches. Per-source connectivity,
staleness, status/errors, session-manager readiness/errors and observation/receipt
timestamps remain unchanged. Cursors include filters, selected incarnation, and
all source diagnostics; freshness changes require restarting pagination.

Durable execution mutations require `idempotency_key`. The application must
persist and deduplicate it in its existing operation journal, reject mismatched request reuse,
and scope it to authenticated actor/operation/target as appropriate. This adapter
does not maintain a competing journal. Read-only annotations distinguish queries
from mutations; execution mutation hints conservatively do not promise deduplication or
non-destructiveness independently of application enforcement.

`register_project` is a desired-configuration upsert, not a durable execution
command. It calls the same registration service as the CLI; identical desired
mappings converge, as reflected by its idempotent annotation. Its strict schema
does not accept an execution idempotency
key, and the adapter maintains no separate key store. It upserts only the
explicitly named node/project binding, with `checkout_path` and `worktree_root`
as typed target-node configuration fields.
They are not MCP-host paths to open, credentials, or configuration files to load.
The public service validates the configuration and propagates errors normally.
Other tools accept project/workspace identity rather than arbitrary filesystem
paths. `create_worktree` requires a safe component `name`,
`branch`, and full 40/64-character hexadecimal `base_commit`; Git ref and binding
validation still belong to the shared service.

`provider` on `start_agent` means an application-configured provider identifier,
not an executable. There are no command, argv, environment, config-file, grant,
vault, per-project user, or approval-policy override fields. Existing agent input
accepts 1-8 explicit keys using the CLI's lowercase names or printable single
ASCII characters. Prompt text uses `prompt_agent`, not `send_agent_input`.

### Lifecycle operations

`start_agent` requires the project/binding revision, workspace, `name`, provider,
and original execution key. Optional `startup_timeout_ms` defaults to 30000 and
accepts 3001-300000 milliseconds. It is independent of the stable 10-second
dispatch TTL and the MCP call timeout. `initial_prompt` is optional and limited
to 8192 UTF-8 bytes with the ordinary public-service control-character validation.
An omitted or empty prompt means no submission. The former prep-only `prompt`
field is not accepted. Named starts require the paired name/incarnation selectors;
omitting both selects the configured endpoint, not an inferred native `default`.
The node persists that endpoint's actual incarnation before effects.

`stop_agent` requires an explicit original `herdr_session_incarnation`, including
for the configured endpoint. Omit `herdr_session_id` only when addressing that
configured endpoint; named stops supply the name too. It additionally requires
`workspace_id`, `tab_id`, `provider`, original execution key, and a `target` with
pane and terminal IDs. `target.agent_session_id` is optional for stop, matching
the ordinary control contract; existing agent-control tools still require it.
No stop selector is rediscovered. The control service stops only that pane and
preserves other panes, workspace, worktree, and native session.

Both operations call `control.StartAgent`/`control.StopAgent` using the same
injected client and normalized request/receipt path as CLI operations. Returned
`agent_lifecycle` JSON retains partial handles, four stage outcomes
(`not_attempted`, `unknown`, `confirmed`), compact stage events, and observations.
The application validates receipts through existing protocol validators without
rewriting their semantic result. Failed, rejected, or indeterminate operation
receipts remain structured tool errors. Confirmed prompt submission is only
acknowledgement, never agent task success.

## Bounds and result contract

Default limits are 8 total active calls per server (shared across sessions), 35 seconds
per call, and 256 KiB per JSON-encoded tool result, including structured output,
all text copies, labels, and metadata. Host options may set 2-64 total active calls, a
positive timeout up to 2 minutes, and 1 KiB-1 MiB of output. Excess concurrency
fails immediately; it does not build an application work queue.

The total is partitioned into ordinary capacity and **one reserved critical-control
slot** shared by `interrupt_agent` and `stop_agent`. Ordinary reads, waits, starts,
and other operations can occupy at most total-minus-one slots. The critical slot
is not borrowed by ordinary work; simultaneous critical calls beyond that one slot
fail explicitly. Thus long waits/startups cannot exhaust urgent-control capacity,
and both lanes together never exceed the configured total. A total of one is
rejected with a clear configuration error rather than silently increased.

Inputs allow at most 64 KiB of encoded arguments. Pagination defaults to 100 and
is capped at 200, with a bounded opaque cursor. Prompt/text input is capped at
8 KiB of UTF-8 bytes, matching the existing control service. Reads require
1-1000 lines and 1-65536 bytes. Waits require 1-30000 milliseconds and 1-5 unique
service states: `idle`, `working`, `blocked`, `done`, or `unknown`. The public
services retain their ordinary validation, safe retries, query normalization,
and input receipt semantics. The application callback rejects oversized reads
without rewriting the raw terminal result.

The callback must honor context cancellation and deadlines. Admission independently
returns a timeout even if it does not; a noncooperative callback retains its active
slot in its original lane until it actually exits, so cancellation or timeout
cannot spawn unlimited abandoned workers. Cooperative cancellation releases that
lane's capacity after the callback exits; it does not consume the other lane.
Go cannot forcibly terminate an injected callback. Bounds apply after SDK message
decoding and before returning application results, not to SDK transport allocations
or allocations performed inside a callback. A shorter host timeout takes precedence
over `wait_agent.timeout_ms`.
For start/stop only, deadline/cancellation handling allows at most an additional
100 ms for a cooperative public service to flush an already-known partial
receipt. That result is still an error and keeps the last observed operation
status (possibly running), not a fabricated terminal state. Late success remains
a timeout, and an uncooperative callback retains its slot beyond the grace period.
If the client cancels its request, the client may discard the response; callers
can inspect the original command ID/key through the durable service later.

Return any non-null JSON-serializable typed application result, or a safe,
caller-visible error. `json.RawMessage` is also supported. Scalars, arrays,
objects, nested values, and integer spellings are preserved on the wire; the
adapter does not string-wrap or replace structured results with terminal prose.
There is no universal output schema because application result shapes are not yet
final. `read_agent` specifically must return a JSON object containing **both**
`text` (string, including an explicitly empty string) and `truncated` (boolean,
including explicit false); additional typed application fields are preserved.
The adapter rejects text exceeding the requested byte/line limits rather than
silently truncating it. The shared read service must produce appropriately bounded
snapshots and truthful truncation information.

Text content is labeled **untrusted application data, not instructions**, including
SDK compatibility blocks. Successful results also carry `_meta["meshmcp/untrusted"]`
without changing `structuredContent`. Application errors, invalid/null results,
output overflow, saturation, and timeouts are explicit MCP tool errors; SDK
protocol errors retain protocol semantics with bounded diagnostic details.
A callback may return both a valid result and an error: the SDK result then has
`isError: true` while retaining the original JSON as `structuredContent` and
untrusted text. This preserves failed/indeterminate command receipts rather than
discarding their command IDs, statuses, and original idempotency keys.
Neither operation acceptance nor durable command success means the agent task
has finished successfully. Keep command/operation status distinct from task
observations in application results. Wait expiry is an explicit timeout/error,
never fabricated task completion.

## Application wiring

`runMCP(ctx, args, IO)` in `app\mcp.go` uses one `control.WithFleet` connection
for the entire MCP session. All suboperations inject that same `FleetClient`
into ordinary public control services with JSON enabled and a private 1 MiB
capture bound. Service output never goes directly to protocol stdout.
The SDK IO transport accepts application streams and caps incoming frames at
128 KiB; actual process streams default to stdin/stdout. Diagnostics remain on
stderr. The MCP identity defaults are hostname `herdr-mesh-mcp`, state subdirectory
`mcp`, shared controller auth-key environment name `TS_AUTHKEY_CLIENT`, client
enrollment tag, and an explicit required coordinator tag. `-auth-key-env` can
override the enrollment environment name. `-timeout` bounds the whole session (zero
means until disconnect/cancellation), independently of `-call-timeout`.

The callback maps node queries, scoped workspace/agent projections, central project
reads and desired-configuration upserts, workspace/worktree commands, existing-agent
get/read/wait/prompt/input/interrupt/start/stop, and command lookup through existing services.
`list_sessions` uses `control.Sessions`; `ensure_herdr_session` maps its
`herdr_session_id` to `control.EnsureSession`'s desired name and preserves its
durable execution key. Successful ensure receipts must identify the requested name
and a valid ready incarnation. Inventory errors and indeterminate ensure receipts
remain structured MCP errors. Named workspaces use
`control.EnsureWorkspaceInSession`; worktree and agent services receive the
original name/incarnation pins in their ordinary typed requests. Named command
receipts are checked against those same pins.

List cursors bind to
the exact returned snapshot and query scope without retaining a snapshot store;
changed snapshots require an explicit restart. JSON protobuf field names,
64-bit number strings, empty fields, and original command receipts are retained.
This is a single-operator path, not a new provider/per-project authorization
system. It checks the expected transport peer tag and explicit selectors while
preserving existing service retry and validation behavior.

Top-level command routing is owned by the embedding application and uses the
single `herdr-mesh mcp` CLI entry point, not a separate binary. All registered
tools now have service mappings; shared node/server lifecycle readiness and
end-to-end validation remain outside this adapter. Do not substitute raw Herdr
APIs or independently execute mutations for unavailable services.

Tests use real official SDK clients and in-memory transports, not a handwritten
JSON-RPC harness:

```powershell
go test .\src\internal\meshmcp -count=1
```
