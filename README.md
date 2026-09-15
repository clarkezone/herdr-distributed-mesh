# herdr-distributed-mesh

Distributed mesh support for Herdr.

## Production foundation

The production application is one binary with explicit process roles:

```powershell
go run ./src/cmd/herdr-mesh server
go run ./src/cmd/herdr-mesh node -server '<server-magic-dns-name>:50052'
go run ./src/cmd/herdr-mesh ctl server-info -server '<server-magic-dns-name>:50052'
go run ./src/cmd/herdr-mesh doctor -server '<server-magic-dns-name>:50052'
go run ./src/cmd/herdr-mesh dashboard -server '<server-magic-dns-name>:50052'
```

`server`, `node`, and `dashboard` are long-running processes. `ctl` and `doctor` are
short-lived clients. Each role uses a separate persistent local state directory
by default. Initial enrollment uses a role-specific environment variable:
`TS_AUTHKEY_SERVER`, `TS_AUTHKEY_NODE`, or `TS_AUTHKEY_CLIENT`. Server/controller
credentials must not be distributed to nodes.

The server verifies every caller through Tailscale `WhoIs` and requires
`tag:herdr-mesh-node` for node streams and `tag:herdr-mesh-client` for control
requests by default. These tags must be granted to the corresponding enrollment
keys in the tailnet policy. If a scratch tailnet retains a wildcard allow ACL,
these application-layer `WhoIs` checks are the role-authorization security
boundary; the added ACL grants alone do not provide network isolation. The
production listener defaults to port `50052`, keeping it separate from the
disposable spike on `50051`.

Use `docs\windows-live-validation.md` for the two-host production transport
gate, including automated tailnet setup, role rejection, restart, network
change, direct-address, MagicDNS re-enrollment, and a short hackathon disruption
loop.

The versioned protocol source is
`api\proto\agentflow\v1\control.proto`. Regenerate Go bindings with:

```powershell
winget install --id Google.Protobuf --exact
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.1
.\scripts\generate-proto.ps1
```

## Monitoring dashboard

The read-only dashboard uses the existing fleet inventory. It is embedded in
the Go binary; no frontend server, npm install, system Tailscale client, or
server/node upgrade is needed for the dashboard itself.

Keep your read-only mesh server and Herdr-enabled node running. Start the
dashboard with an **unused enrolled client state directory**, then open
**http://127.0.0.1:8787**. For the setup used by the validation runbook, the
short-lived doctor's identity can be reused:

```powershell
.\herdr-mesh.exe dashboard `
  -hostname herdr-mesh-doctor `
  -state-dir "$env:LOCALAPPDATA\herdr-mesh-validation\doctor" `
  -server '<server-magic-dns-name>:50052'
```

Keep that process running. Do not run `doctor` or `ctl` with the same state
directory while the dashboard owns it. A separately enrolled dashboard identity
can use the default `dashboard` state directory instead; initial enrollment
uses `TS_AUTHKEY_CLIENT` and `tag:herdr-mesh-client`.

The page shows fleet connectivity, Herdr readiness/freshness, agent statuses,
and workspace/tab/pane inventory. It refreshes automatically and supports
filtering. Loading, an empty fleet, unavailable nodes, and failed server queries
are distinct states; previously displayed data is marked not-live on failure.
Names, paths, custom metadata, and terminal contents remain excluded.
In the current projection, an agent's identifier is its pane ID; the dashboard
links it to the matching pane within the same node, workspace, and tab.

The local web gateway connects to the coordinator using embedded tsnet and the
same authenticated `Fleet.ListNodes` RPC as `ctl nodes`. It binds only to a
literal loopback IP (`-listen 127.0.0.1:8787` by default), checks the browser's
Host/Origin, disables caching and cross-origin API access, and exposes no
mutation endpoints. It trusts local processes/users on that host; it is not a
public HTTP service or a substitute for OS user isolation. If the port is
occupied, choose another loopback port with `-listen`.

Delivery order: **dashboard first**, then durability/command safety and
orchestration, then a **fuller standalone CLI**, then **MCP as a separate
integration**. CLI functionality must not depend on MCP.

Dashboard checks (frontend tests use only Node's built-in test runner):

```powershell
go test ./src/internal/dashboard ./src/internal/app
node --test src\internal\dashboard\web\model.test.mjs
```

## Read-only Herdr integration

The node can opt in to `ping`, `events.subscribe`, and `session.snapshot` against
a local Herdr instance. Without `-herdr-socket`, it remains transport-only.
Upgrade/restart the mesh server before starting an integration-enabled node;
the node rejects servers that do not advertise `herdr.read.v1`.

Build the binary, then restart your node with the same enrolled identity/state:

```powershell
go build -o .\herdr-mesh.exe .\src\cmd\herdr-mesh
.\herdr-mesh.exe node `
  -server '<server-magic-dns-name>:50052' `
  -state-dir "$env:LOCALAPPDATA\herdr-mesh-validation\node" `
  -herdr-socket "$env:APPDATA\herdr\herdr.sock"
```

Stop processes before overwriting their binary on Windows, or build to another
filename and use that executable for the restarted roles.

Use your existing node hostname if it was explicitly configured. Do not run
two processes sharing a state directory. On Windows the socket argument is
Herdr's marker path, mapped to a **local named pipe**, not a TCP endpoint.
Custom sessions require their own socket path. Read-only integration does not
launch or modify Herdr processes; workspace creation requires the separate
explicit project policies described below.

Query from a client-tagged identity (reuse your enrolled controller state):

```powershell
.\herdr-mesh.exe ctl nodes `
  -server '<server-magic-dns-name>:50052' `
  -state-dir "$env:LOCALAPPDATA\herdr-mesh-validation\doctor" `
  -json
```

Omit `-json` for a compact count/status summary. JSON is a single object with a
`nodes` array and snake_case protobuf field names; uint64 sequences are strings.
Each node includes connectivity, last heartbeat, Herdr status, snapshot receipt
time, and a `stale` flag. Status is `disabled`, `waiting`, `ready`, or
`unavailable`; failures expose a sanitized category rather than local API text.
`stale` is true unless a connected node has a ready snapshot received within
30 seconds. Herdr traffic never extends the heartbeat deadline.

Only entity IDs, workspace/tab relationships, focus flags, and agent status are
forwarded. Titles, labels, paths, terminal output, agent names/session metadata,
and custom tokens are excluded on the node. The server also validates the
projection and rejects unknown protobuf fields. This is not a complete layout
or terminal mirror. Observation alone enables **no remote Herdr mutation or
arbitrary command execution**.

Herdr protocol 18 is the initial supported local contract. The observer waits for
subscription acknowledgement before taking its baseline. Events trigger
coalesced, rate-limited authoritative snapshots; a five-second refresh also
covers status changes and missed events. There is no global snapshot cursor, so
events are never replayed as deltas over newer state. Connection loss reports
`unavailable`; reconnect takes a fresh baseline. This is eventually consistent,
not a lossless event log.

The contract is checked against `herdr api schema --json` and the
[Herdr Socket API documentation](https://herdr.dev/docs/socket-api/).
Protocol 18 subscription selectors are dotted (`pane.updated`), but streamed
event discriminators are snake_case (`pane_updated`). Unsupported Herdr
protocol versions report `unavailable` until the adapter is updated.

Fleet state is capped at 128 nodes, 256 KiB per projected state, 4,096 entities
per state, and 2 MiB total projected payload. Limits fail explicitly, not by
truncating data. The latest redacted observations and identity bindings are
persisted in SQLite. On server restart, observations return **disconnected and
stale**, never apparently live. A reconnect requires a new baseline before
freshness is restored. New node streams fence out older streams for the same
bound identity.

## Durable coordinator state

The server stores its database at `<server-state-dir>\coordinator\mesh.db`.
It uses a pure-Go SQLite driver, so no database service or C compiler is needed.
Restart only the server with the new binary and its existing `-state-dir` to
enable this storage; existing read-only nodes and dashboard clients remain
compatible.

On startup, both legacy `node-bindings.jsonl` formats (JSON array or JSONL) are
imported transactionally without changing the original file. Repeated imports
are idempotent; conflicts or malformed data stop startup rather than discarding
bindings. The database is tied to the server's persisted instance ID and is
protected against concurrent coordinator processes using an OS-held lock.

Identity binding and each latest fleet update commit before they become
visible to clients. Storage failures reject new operations and stop the
coordinator; there is no fallback to apparently healthy in-memory state.
Database corruption, unsupported schema versions, and identity mismatches
require explicit operator attention and never trigger an automatic reset.

This stores **latest observations, not event history**. The existing retention
policy is unchanged: disconnected observations expire after 15 minutes without
a heartbeat, even across restarts. Expiry never removes the durable identity
binding. Admitted command transitions have a separate durable journal, described
below. Project bindings and mutation-specific execution safety remain subsequent
work beyond the scoped workspace-ensure operation below.

The dedicated coordinator directory is private to the current user (plus
SYSTEM on Windows), including database sidecars. Keep it on a local filesystem.
SQLite uses WAL with full synchronization. Do not remove its lock/WAL files or
copy only `mesh.db` while the server runs. For an offline backup, stop the server
and copy the entire server state directory, including its instance ID, tsnet
identity, and coordinator directory. Downgrading to a JSON-binding-only server
is not supported: it would ignore bindings learned by the SQLite-backed server.

Durability and recovery checks:

```powershell
go test ./src/internal/state ./src/internal/server
```

Local automated coverage and optional read-only checks against a running Herdr:

```powershell
go test ./src/internal/...
$env:HERDR_MESH_TEST_SOCKET = "$env:APPDATA\herdr\herdr.sock"
go test ./src/internal/herdr ./src/internal/node ./src/internal/server -run Live -count=1
Remove-Item Env:\HERDR_MESH_TEST_SOCKET
```

The remaining two-host restart/NIC/sleep/hostname-collision runbook is
`docs\windows-live-validation.md`. That gate is deferred, not passed, and remains
required before the two-machine demo. Worktree creation, broader mutations,
durable event history, the fuller standalone CLI, and MCP remain later phases.

## Journaled command-safety probe

Without workspace policies, the only admitted command is **`node.ping.v1`**, which returns `pong`. It never
calls Herdr or executes shell commands. This exercises the command delivery and
recovery path before introducing actual workspace/worktree mutations.

Upgrade the server first, preserving its enrolled state. Coordinator schema 1
upgrades transactionally through schema 2 to schema 3, preserving bindings,
observations, and probe records. The node journal similarly upgrades from schema 1
to schema 2. Older journal-aware binaries reject these newer schemas; take an
offline backup before upgrading.
Restart the node with the new binary, its existing hostname/state/socket flags,
and **`-enable-probes`**. Probes are off by default and do not require Herdr.
An enabled node requires a coordinator advertising `commands.node-ping.v1`.
Its protected journal lives at `<node-state-dir>\commands\journal.db`.

Probe-enabled nodes and probe CLI clients verify the actual connected peer has
`tag:herdr-mesh-server`, including every reconnect. Change this only with
`-required-server-tag` when using a different server tag. The server independently
requires `-required-command-tag` (default `tag:herdr-mesh-client`) and assigns the
actor from authenticated WhoIs identity, not request input. This default permits
client-tagged identities to ping, not to perform arbitrary operations.

From an **unused enrolled client identity**, submit a probe and wait for its
outcome, or retrieve an earlier command:

```powershell
.\herdr-mesh.exe ctl ping `
  -server '<server-magic-dns-name>:50052' `
  -state-dir '<unused-enrolled-client-state-dir>' `
  -node '<instance-id-from-ctl-nodes>' `
  -idempotency-key smoke-ping-1 `
  -json

.\herdr-mesh.exe ctl command `
  -server '<server-magic-dns-name>:50052' `
  -state-dir '<same-client-state-dir>' `
  -id '<command-id-from-ping>' `
  -json
```

Keep any explicitly configured client hostname unchanged. Do not share the
dashboard's state directory while it is running. `ctl nodes -json` reports
`command_ready`; this means a negotiated, opted-in **probe** session, not general
mutation readiness. Older read-only nodes remain usable for monitoring.

The CLI prints its idempotency key to stderr before submitting; JSON stdout is
one command record with status and audit transitions. If the connection fails
or the CLI wait times out, repeat `ctl ping` with the **same client identity,
node, idempotency key, and TTL**. A matching retry returns the existing command,
even if the node is now offline; changing the request under that key fails.
Omitting the key generates a new one. Keys are request identifiers, never
Tailscale enrollment secrets. Command lookup is scoped to the submitting actor.

`-ttl` defaults to 10 seconds and cannot exceed 30 seconds. It is an execution
deadline, distinct from the CLI's `-timeout` (default 60 seconds). Dispatch intent
commits before sending; node execution intent commits before returning `pong`.
Node results remain pending until the coordinator commits and acknowledges them.
Acknowledgements confirm receipt of the node's result status. Replayed uncertainty
does not overwrite a known coordinator outcome; `ctl command` remains authoritative.
Replacement sessions wait for the old stream and its registered sends to finish; supersession
interrupts pending sends, and command traffic never extends heartbeat deadlines.
Already-transmitted commands cannot be revoked by replacing a session.
Restarted nodes replay known results, not execution. Interrupted execution or a
lost result can produce **INDETERMINATE**, not a fabricated failure/success.
A later authenticated result can reconcile that status; query the same command
instead of assuming a fresh idempotency key is a safe retry for future mutations.

Each coordinator and node journal retains at most **4,096 commands**, including
deduplication tombstones. Capacity failures are explicit; records are never
silently evicted. Retention/maintenance tooling remains future work. Do not
delete or replace journals to recover capacity: that discards retry protection.
Back up the whole node state offline, just as for the coordinator.

The durable audit covers **admitted command transitions**, not all denied
requests. Denials are not a durable security audit. This is a probe-only safety
foundation when no workspace policy is configured, not arbitrary mutation
authorization, automated reconciliation, the fuller CLI, or MCP.

## Project-scoped workspace ensure

`ctl ensure-workspace` is the first real Herdr mutation. It ensures a workspace
for one **explicitly bound local Git checkout**: reuse the matching workspace, or
create one with `focus:false`. It does not create worktrees, clone repositories,
send prompts, inject commands/environment variables, rename or close workspaces.
Creating a Herdr workspace may start its normal local terminal; only bind
checkouts whose local startup behavior you trust.

Enable it by supplying **both** a coordinator and node `-workspace-policy` file.
No policy means no workspace mutations. Policies are immutable for a process
lifetime; edit them locally and restart the corresponding role to apply changes.
They contain explicit actor allowlists, never wildcard grants.
Keep policies outside the repository; `*.workspace-policy.json` is also ignored
to help prevent accidental commits of machine-local bindings.

Coordinator policy (no checkout paths):

```json
{
  "projects": [
    {
      "project_id": "AgentFlow",
      "node_id": "replace-with-mesh-node-instance-id",
      "binding_revision": "r1",
      "actor_ids": ["replace-with-client-tailscale-stable-id"]
    }
  ]
}
```

Node policy repeats those exact fields and adds `"path": "C:\\dev\\AgentFlow"`
to the project entry. Use a real existing Git checkout on that node, not this
example path. `node_id` comes from `ctl nodes`; the actor is the client's
**Tailscale stable ID**, not its hostname, account name, or mesh instance ID.
The doctor reports that identity with its local diagnostics. Do not run it using
state currently owned by the dashboard or another process.

The project ID is a stable logical key, such as the established AI Core project
key; it is not inferred from a checkout path or workspace label. These policies
are explicit local bindings, not automatic vault synchronization. Local paths,
enrollment credentials, and machine runtime state must not enter shared AI Core
memory. Each policy is limited to 64 KiB and 128 bindings; duplicate bindings,
checkout aliases, unknown JSON fields, mismatched node IDs, and invalid paths
fail startup. Node bindings pin directory identity and recheck it before use.
Change `binding_revision` on **both** policies whenever changing a binding.

Upgrade/restart the server first with its existing identity flags plus
`-workspace-policy '<server.workspace-policy.json>'`. Restart the node with its
existing identity/socket flags plus `-workspace-policy '<node.workspace-policy.json>'`.
A node policy requires `-herdr-socket`, enables the durable journal automatically,
and also retains ping support; `-enable-probes` is not additionally required.
The existing dashboard can remain running with its own enrolled state.

From an unused enrolled client identity:

```powershell
.\herdr-mesh.exe ctl ensure-workspace `
  -server '<server-magic-dns-name>:50052' `
  -state-dir '<unused-enrolled-client-state-dir>' `
  -node '<mesh-node-instance-id>' `
  -project AgentFlow `
  -binding-revision r1 `
  -idempotency-key ensure-AgentFlow-1 `
  -ttl 30s `
  -json
```

Preserve any explicitly configured client hostname. Success returns
`workspace_ensure` with `project_id`, `binding_revision`, `workspace_id`, and
`created`. Neither paths nor Herdr labels, terminal data, or raw errors are
returned. Human output includes the same identifiers. `ctl command` retrieves
the durable outcome; retries must retain the same actor, key, target, binding,
and TTL. Reusing that key returns the original operation, not a new check after
someone closes its workspace.

`ctl nodes -json` exposes `workspace_ready` separately from probe `command_ready`.
Admission requires negotiated workspace support and a fresh ready Herdr baseline.
The coordinator rechecks the client's and node's WhoIs roles before dispatch;
the production node checks the actual coordinator peer again before its effect.
Both sides enforce the project's actor/revision binding. Workspace execution is
serialized on the node without blocking heartbeats or observer traffic.

**Uncertainty is not retried.** A timeout, malformed response, or connection loss
after attempting creation can mean Herdr created the workspace. The command is
then `INDETERMINATE`; after interrupted intent, restart also remains indeterminate.
The node durably rejects new workspace keys for that project as
`project_unresolved`, across actors and binding revisions, until explicit future
reconciliation tooling resolves the ambiguity. Changing keys, resetting the
journal, or rebinding the same project is not a safe workaround.

Matching uses local checkout identity, never labels. Multiple existing matches
fail as `ambiguous_workspace`. Herdr has no atomic ensure/idempotency API:
external local clients can still race creation, and filesystem validation cannot
eliminate the gap before path-based IPC. Avoid concurrent external creation or
checkout replacement while ensuring a workspace. This milestone does not claim
global uniqueness, automatic reconciliation, or an exactly-once Herdr effect.

Workspace coverage includes fake local IPC with the real node/journals on
Windows; it does not mutate existing user workspaces or replace the deferred
two-host disruption validation.

## Windows tsnet feasibility spike

The first milestone is intentionally disposable. It tests whether two Windows
processes can embed Tailscale with `tsnet` and exchange bidirectional gRPC
messages without a separately installed Tailscale daemon. It does not contain
Herdr integration or production mesh architecture.

Use a development Tailscale auth key that may enroll both nodes, or separate
keys. Keep keys in environment variables and never commit them.

On the server machine:

```powershell
$env:TS_AUTHKEY_SERVER = '<development-auth-key>'
go run ./src/cmd/spike server `
  -hostname herdr-mesh-spike-server `
  -state-dir "$env:LOCALAPPDATA\herdr-mesh-spike\server"
```

After the server is enrolled, use its full MagicDNS name on the client machine:

```powershell
$env:TS_AUTHKEY_CLIENT = '<development-auth-key>'
go run ./src/cmd/spike client `
  -hostname herdr-mesh-spike-client `
  -state-dir "$env:LOCALAPPDATA\herdr-mesh-spike\client" `
  -server 'herdr-mesh-spike-server.example.ts.net:50051'
```

Each line entered by the client is sent through a bidirectional gRPC stream and
returned as a pong. If the stream fails, the client retries the same message
until the server becomes reachable or Ctrl+C is pressed.

The state directories contain persistent tsnet node identities. Keep the server
and client directories separate, do not share them between machines, and retain
them when testing restart behavior. If the state is already enrolled, the auth
key environment variable may be omitted.

Optional flags:

```text
-auth-key-env <name>  Environment variable containing the auth key
-debug                Enable verbose tsnet logs
-listen <address>     Server listener, default :50051
-retry-delay <value>  Client reconnect delay, default 2s
-tags <tag:a,tag:b>   Tailscale tags requested during enrollment
```

Run the local protocol test:

```powershell
go test ./src/cmd/spike
```

The actual feasibility decision requires the server and client to run on two
Windows machines on the same tailnet, followed by restart and temporary network
loss tests.

That feasibility test has passed. The spike remains isolated from production
packages. Removing it is a post-gate remaining action after the production
transport validation is complete; it is intentionally retained during this
gate.
