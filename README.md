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
Custom sessions require their own socket path. No Herdr processes are launched
or modified by the mesh.

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
or terminal mirror and **does not support remote mutation or command execution**.

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

Fleet state is in memory, capped at 128 nodes, 256 KiB per projected state,
4,096 entities per state, and 2 MiB total projected payload. Limits fail
explicitly, not by truncating data. Disconnected nodes expire after 15 minutes
without a heartbeat; a server restart clears inventory, and nodes repopulate
it on reconnect. New node streams fence out older streams for the same bound
identity.

Local automated coverage and optional read-only checks against a running Herdr:

```powershell
go test ./src/internal/...
$env:HERDR_MESH_TEST_SOCKET = "$env:APPDATA\herdr\herdr.sock"
go test ./src/internal/herdr ./src/internal/node ./src/internal/server -run Live -count=1
Remove-Item Env:\HERDR_MESH_TEST_SOCKET
```

The remaining two-host restart/NIC/sleep/hostname-collision runbook is
`docs\windows-live-validation.md`. That gate is deferred, not passed, and remains
required before the two-machine demo. Remote mutations, durable event history,
the fuller standalone CLI, and MCP remain later phases.

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
