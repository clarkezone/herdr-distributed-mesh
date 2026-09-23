# herdr-distributed-mesh

Connect computers over embedded Tailscale, discover headless Herdr sessions,
and run and observe agents against centrally registered project checkouts.
The mesh is designed for one trusted operator; it is not a multi-tenant service
or a sandbox for untrusted repositories.

## Start here

The product has one public executable and command: **`herdr-mesh`**
(`herdr-mesh.exe` on Windows). Install it on `PATH` and use its subcommands;
server, node, controller, dashboard, MCP, and maintenance are not separate binaries.
Start with the [two-computer operator guide](docs/operator-guide.md).
Managed setup uses one background mesh connection per computer; diagnostics,
the dashboard, MCP, and commands share it.

On the first computer:

```powershell
herdr-mesh init --tailnet example.com --name desktop
```

On the second computer, use the exact full MagicDNS address printed by setup:

```powershell
herdr-mesh join --server herdr-mesh-desktop.example.ts.net
```

The address above is an example, not an address to invent from the hostname.
On the controller, **`herdr-mesh help`** and **`herdr-mesh status`** show its
actual assigned join command, including any nondefault port. Help reads saved
state without starting or enrolling anything. If browser sign-in or device
approval finishes after setup stops waiting, check `status`, then retrieve
the join command with `help`; do not recreate the mesh.

From either computer afterward:

```powershell
herdr-mesh nodes
herdr-mesh doctor
herdr-mesh dashboard
```

Register an existing checkout on the execution computer and launch a task:

```powershell
herdr-mesh project add demo --node laptop --path C:\src\demo
herdr-mesh agent start smoke --node laptop --project demo --prompt "Say hello"
herdr-mesh agent follow smoke --node laptop
herdr-mesh agent stop smoke --node laptop
```

Replace `laptop` with its label from `nodes`. Checkout paths belong to that
computer. Start ensures a headless `main` Herdr session and an existing-checkout
workspace, then launches Copilot by default; it does not create a worktree.
No attached Herdr terminal is required. Canceling follow stops watching, not
the agent. An agent name remains bound to its original launch: repeat the exact
start to inspect/retry that request, not to launch a replacement.
Prompt receipt and provider readiness do not establish task completion or success.

First-time tailnet configuration prompts for an API access token privately.
Device connection uses browser sign-in; normal setup does not require counting
or distributing enrollment keys. Herdr, Git, and the intended authenticated
provider must be installed. Managed `init`, `join`, and `start` run a background
process on Windows, Linux, and macOS that survives closing the launching terminal.
Mesh does not use the Windows registry or install sign-in autostart, boot
services, or scheduled tasks. After reboot or shutdown, start it explicitly.

`--name` is optional for `init` and `join`. By default, the OS hostname is
normalized to lowercase letters, digits, and hyphens, with a prefix if needed
to start with a letter and a 40-character cap. Use `--name` to override it;
the printed join command does not require a name.

Configuration, databases, journals, tsnet identity, and logs live in
**`herdr-mesh-state` beside the executable**, not in the working directory or
AppData. Install into a writable, private local directory. An optional global
override goes **before** the command:

```powershell
herdr-mesh --state-dir C:\private\mesh-state start
```

Use the same absolute override for `help`, `init`, `join`, `shutdown`, `status`, `nodes`,
`dashboard`, `mcp`, `project`, and `agent` when selecting that installation.
Do not sync or run copies of tsnet state on multiple computers. A OneDrive copy
of the binary is a delivery artifact, not a recommended live installation;
install outside synced folders.

Normal startup and policy setup support installer-managed parent-directory
links, including Herdr's Windows `bin` junction. The actual state directory and
private files must remain ordinary directories/files. Destructive maintenance
keeps stricter checks: select the physical, non-aliased state path for cleanup.

Stop the managed daemon without destroying any state:

```powershell
herdr-mesh shutdown
```

Resume the saved installation without repeating join, name, or server flags:

```powershell
herdr-mesh start
```

`start` reads the selected saved configuration and launches the current
executable. After shutdown, replace the binary in place and run `start` to
upgrade. Copying only the binary elsewhere selects fresh default state; to
relocate an installation on the same computer, move the whole installation
directory while stopped or explicitly select its state with the global
override. The saved state does not depend on the previous executable location.

For deliberate deinitialization and a clean start, use
`herdr-mesh shutdown --destroy`; add `--remove-policy` on the coordinator to
request removal of provably owned, unused policy additions. Preview with
`--dry-run`. Destruction requires typed confirmation (or explicit `--yes`) and
private Tailscale API authorization. Local identity/recovery data is retained
until remote cleanup is confirmed. Herdr sessions, agents, repositories,
worktrees and other computers are not deleted. See the operator guide for
partial-cleanup recovery and the distinction from `agent stop`.
Deleting a local folder alone does not unregister remote Tailscale devices or
remove tailnet policy. There is no silent AppData migration: shut down and
destroy an old installation with its old version before a clean setup. This
version does not read, migrate, or delete old startup registration.

## Validation and release scope

CI runs production and archived-experiment tests, vet, dashboard model tests,
and CLI builds on Windows, Linux, and macOS; Linux also runs race checks.
Windows runs the mocked policy/bootstrap automation boundaries. Releases are
six single-binary archives (amd64/arm64 for each OS), with checksums.

Windows browser enrollment, managed restart, native Herdr discovery, and
read-only live controller/dashboard access have been exercised. Automated
coverage and cross-builds do **not** establish all-platform live acceptance:
the full two-host disruption/provider matrix, destructive live Tailscale API
cleanup, and release signing remain release gates. See
[Windows operational acceptance](docs/windows-live-validation.md).
Unknown mutation outcomes retain their retry and reconciliation fences; never
clear journals or change identities/keys to force a retry.

## Advanced and developer reference

The remainder describes advanced explicit roles and implementation-level
validation. It is not a sequence of extra steps required after `init`/`join`.
See the [advanced operator guide](docs/advanced-operator-guide.md) for those modes.

Tailnet setup (`herdr-mesh setup tailnet`) and prepared-endpoint Windows bootstrap
(`herdr-mesh bootstrap`) are embedded in that same executable. The operator guide
uses installed CLI commands; source build/test examples below are developer
instructions, not additional product entrypoints. Durable agent launch/stop is
available through `ctl agent start` and `ctl agent stop`; real operator/provider
and platform acceptance remain separate gates. Herdr, Git,
PowerShell, OpenSSH, and provider runtimes remain explicit dependencies where
needed; their existence does not create another mesh CLI product.

`server`, `node`, and `dashboard` are long-running processes. `ctl` and `doctor` are
short-lived clients. Each advanced role uses a separate persistent local state
directory under `<executable-directory>\herdr-mesh-state\advanced\<role>` by
default. These are separate from the shared managed installation. Initial
enrollment uses a role-specific environment variable:
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

Use [Windows operational acceptance](docs/windows-live-validation.md) for the
advanced two-host release-validation matrix, including role rejection, network
changes, explicit target pins, and restart/recovery checks. Normal first use
does not require completing that matrix.

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
the Go binary; no frontend server, npm install, or system Tailscale client is
needed. Use the same mesh build on the controller and execution nodes.

Keep the mesh controller and execution nodes running. After `init` or `join`,
start the dashboard and open **http://127.0.0.1:8787**:

```powershell
herdr-mesh dashboard
```

Keep the dashboard process running. The normal command shares this computer's
saved mesh connection and does not enroll another Tailscale identity.
Explicit `dashboard --server <address>` is an advanced, separately enrolled
client mode; see the advanced operator guide before using it.

The page shows fleet connectivity, per-session Herdr readiness/freshness, agent
statuses, provider/readiness metadata, and workspace/tab/pane inventory. It
refreshes automatically and supports project/provider/readiness filtering.
Loading, an empty fleet, unavailable nodes, and failed server queries
are distinct states; previously displayed data is marked not-live on failure.
Configured names and reported working/checkout directories are displayed and
searchable, with IDs secondary or as fallbacks. Workspace/tab labels remain
visible for agents that have no configured name of their own. Automatic terminal
titles, custom metadata, provider session-file paths, and terminal contents remain
excluded. Upgrade the coordinator before nodes, then restart the dashboard to
receive the new fields; older nodes retain ID-only fallbacks.
In the current projection, an agent's identifier is its pane ID; the dashboard
links it to the matching pane within the same node, Herdr session/incarnation,
workspace, and tab. Optional metadata requires a node that reports it; unknown
readiness is not treated as false.

The local web gateway uses the same authenticated fleet inventory RPC as the
CLI, through shared local IPC in managed mode and embedded tsnet in explicit
client mode. It binds only to a
literal loopback IP (`-listen 127.0.0.1:8787` by default), checks the browser's
Host/Origin, disables caching and cross-origin API access, and exposes no
mutation endpoints. It trusts local processes/users on that host; it is not a
public HTTP service or a substitute for OS user isolation. If the port is
occupied, choose another loopback port with `-listen`.

CLI, dashboard, and MCP are independent interfaces over the same control
protocol; CLI functionality does not depend on MCP. Human CLI output uses
readable labels and recovery guidance. Automation should use the documented
JSON or MCP interfaces rather than parse human text.

Dashboard checks (frontend tests use only Node's built-in test runner):

```powershell
go test ./src/internal/dashboard ./src/internal/app
node --test src\internal\dashboard\web\model.test.mjs
```

For an isolated browser check, set `HERDR_MESH_DASHBOARD_FIXTURE=127.0.0.1:18787`
in a disposable shell and run `go test ./src/internal/dashboard -run
^TestDisplayBrowserFixture$ -v -count=1 -timeout=6m`. It serves synthetic names
and directories through the production dashboard for five minutes, without
connecting to a daemon or tailnet. Clear the variable afterward.
The separately opt-in `HERDR_MESH_DISPLAY_LIVE=1` test
`TestLiveDisplayMetadata` reads an existing native Herdr snapshot without
modifying sessions and reports counts only, never names, paths or raw snapshots.

## Herdr connection and observation

The node uses `ping`, `events.subscribe`, and `session.snapshot` against
a local Herdr instance. Without either `-herdr-socket` or `-herdr-executable`,
an advanced explicit-role node remains transport-only. Managed `init`/`join`
configure discovery through the installed Herdr executable automatically.
**Supplying `-herdr-socket` also enables authenticated existing-agent control**
and its durable journal, described below. There is no additional agent
allowlist or project policy to configure.
Upgrade/restart the mesh server before starting an integration-enabled node;
the node rejects servers that do not advertise the required observation and
agent-control capabilities (`herdr.read.v1` and `herdr.agent-control.v1`).

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
The socket remains the explicit legacy/default target. To discover and ensure
named headless sessions, configure `-herdr-executable` as described below;
that mode can start with no default socket at all. Workspace creation uses
the centrally registered project checkouts described below.

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

Only validated entity IDs, workspace/tab relationships, focus flags, agent status,
named-session names/incarnations, project/provider IDs, optional readiness,
terminal IDs, and opaque provider-session IDs are forwarded. Display metadata
adds configured workspace/tab/pane labels and agent names (at most 256 UTF-8
bytes), plus reported working/checkout directories (at most 4096 UTF-8 bytes).
These are display-only, never identity or authorization selectors. Foreground
working directories take precedence over launch cwd when reported. Automatic
terminal titles, terminal output, provider session-file references, repository
internals and custom tokens are excluded on the node. The server also validates the
projection and rejects unknown protobuf fields. This is not a complete layout
or terminal mirror. Explicit agent queries and typed input commands use separate
RPCs; terminal text is never included in these fleet observations.

Herdr native JSON API protocols **18, 20 and 22** are explicitly supported, including
Herdr **0.8.2** (protocol 20) and **0.9.1** (protocol 22). Discovery, observation and mutation adapters share
one allowlist; unverified versions, including 19 and 21, remain rejected. Native CLI
`compatible: true` is necessary for managed session discovery but is not a
substitute for this adapter check. Mesh protocol 1 is unrelated.

The observer waits for
subscription acknowledgement before taking its baseline. Events trigger
coalesced, rate-limited authoritative snapshots; a five-second refresh also
covers status changes and missed events. There is no global snapshot cursor, so
events are never replayed as deltas over newer state. Connection loss reports
`unavailable`; reconnect takes a fresh baseline. This is eventually consistent,
not a lossless event log.

The contract is checked against `herdr api schema --json` and the
[Herdr Socket API documentation](https://herdr.dev/docs/socket-api/).
Supported subscription selectors are dotted (`pane.updated`), but streamed
event discriminators are snake_case (`pane_updated`). Unsupported Herdr
protocol versions report `unavailable` until the adapter is updated.

Compatibility was checked against the official
[protocol-18 preview](https://github.com/herdrdev/herdr/tree/44b3adb125524ea9a55739eee3776f922f2115ad)
and releases [v0.8.2](https://github.com/herdrdev/herdr/tree/9eb521456ac0d19d3ab3d9d7cea3cca10baa8a4c)
and [v0.9.1](https://github.com/herdrdev/herdr/tree/065ef9d6a531c49fb8bee7e818ef837065b21ee9)
source contracts (`src/api/schema`, `src/cli/status.rs`, `src/session.rs`,
`src/ipc.rs`), plus the official Windows 0.8.2/0.9.1 offline schemas and isolated
read-only discovery commands. The intervening native protocol bumps affect
binary terminal transport; this adapter uses the verified JSON API subset.
Fixtures exercise all three protocols across session discovery, snapshots/events,
workspace/worktree operations and agent lifecycle/control. This is not a claim
of live two-machine acceptance on every native release. Keep terminal/session
identity checks, readiness polling, and no-retry-on-uncertain-mutation behavior:
none provides expected-terminal compare-and-swap.

Herdr 0.9.1's endpoint generation and `endpoint_compatible` are separate
client-shell contracts, not a new handshake or compatibility gate for this
JSON adapter. Subscription history is not replayed; the adapter subscribes
before taking an authoritative baseline snapshot. Delayed prompt acknowledgements
still do not establish task completion; cancellation after submission remains
indeterminate and never triggers a retry. The mesh does not escalate a refused
pane close to workspace-group close. It also leaves `trust_repository` omitted:
Git ownership checks remain enforced, with no implicit `safe.directory` override.

### Native discovery diagnostics

If a joined node is connected but Herdr is not live, collect these read-only
diagnostics on the affected Windows computer. `herdr` here is the installed
native prerequisite, not another mesh executable:

```powershell
herdr-mesh version
Get-Command herdr | Select-Object -ExpandProperty Source
herdr --version
herdr session list --json
herdr status server --json
$meshDirectory = Split-Path -Parent (Get-Command herdr-mesh -CommandType Application).Source
Get-Content (Join-Path $meshDirectory 'herdr-mesh-state\daemon.log') -Tail 200
```

If you selected a global `--state-dir`, read `daemon.log` in that directory
instead; the current working directory does not select the managed log.

For a named session, also use `herdr --session main status server --json`,
replacing `main` with its actual name. The default session omits `--session`.
An updated executable is not proof that an already-running Herdr server has
upgraded: inspect its reported version, protocol and compatibility. Do not
reset enrollment or destroy state. Remove sign-in URLs, credentials and private
paths before sharing diagnostics.

### Fleet limits and recovery

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
below. Durable execution includes the scoped workspace/worktree operations,
headless session startup, and agent lifecycle/control described below.

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

The full two-host restart/NIC/sleep/hostname-collision runbook is
`docs\windows-live-validation.md`. That release-acceptance gate is not claimed
complete. Durable event history and automatic reconciliation of unknown effects
are not implemented; standalone CLI and MCP control are available.

## Journaled command-safety probe

The read-only probe command is **`node.ping.v1`**, which returns `pong`. It never
calls Herdr or executes shell commands. This exercises the command delivery and
recovery path before introducing actual workspace/worktree mutations.

Upgrade the server first, preserving its enrolled state. Coordinator schema 1
upgrades transactionally through schemas 2 and 3 to schema 4, preserving bindings,
observations, and probe records. The node journal similarly upgrades from schema 1
through schema 2 to schema 3. Older journal-aware binaries reject these newer schemas; take an
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
Tailscale enrollment secrets. Same-key submission retries remain scoped to the
original submitting actor. Any client authorized for command access can inspect
a known command ID with `ctl command`, including commands submitted by another
client; lookup does not transfer the original actor's retry-key scope.

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
silently evicted. Offline inspection and full-role backup are available through
`maintenance`; automatic pruning and uncertainty reconciliation are not. Do not
delete or replace journals to recover capacity: that discards retry protection.
See [journal maintenance](docs/journal-maintenance.md) for advanced role backups.
For managed installations, preserve the complete stopped installation instead.

The durable audit covers **admitted command transitions**, not all denied
requests. Denials are not a durable security audit. Probe-only nodes cannot
perform Herdr mutations. This is not arbitrary mutation authorization,
automatic reconciliation, the fuller CLI, or MCP.

## Named headless sessions

Herdr session names preserve their exact letter case, including existing names
such as `QEI`. The mesh accepts `[A-Za-z0-9][A-Za-z0-9_-]{0,63}` and rejects
Windows device names case-insensitively. This differs from mesh node labels,
which remain lowercase. A stopped mixed-case session does not hide healthy
sessions. Upgrade both coordinator and affected nodes to carry these names
through discovery, control, and the dashboard.

Upgrade the coordinator and node together. Enable the named-session manager
with an **explicit** executable; no installed-default or focused session is
selected implicitly, and no default Herdr process needs to be running:

```powershell
.\herdr-mesh.exe node -server '<server>:50052' `
  -state-dir '<existing-node-state-dir>' -herdr-executable 'C:\Herdr\herdr.exe'
.\herdr-mesh.exe ctl sessions -server '<server>:50052' -node '<node-id>' -json
.\herdr-mesh.exe ctl session ensure -server '<server>:50052' -node '<node-id>' `
  -name worker -key worker-start-1 -ttl 30s -json
.\herdr-mesh.exe ctl sessions -server '<server>:50052' -node '<node-id>' -json
```

Use the node's existing hostname/identity and an unused enrolled client state
directory as in the examples above. `-herdr-executable` enables the private node
command journal, authenticated coordinator-tag checks, managed project
configuration, and agent controls even without `-herdr-socket`.
Keeping `-herdr-socket` preserves its configured/default route. With no socket,
an empty session selector has **no default fallback**: it never means the
focused session or an installed default. Select the name explicitly.
Ensuring `default` when a custom unmanaged default socket is configured is
rejected as `session_default_unmanaged`; it is not silently redirected.

`ctl sessions` returns a typed `sessions` array plus `error_code`, not native
paths, socket markers, or raw process diagnostics. Inventory entries expose
`name`, `incarnation`, `status`, `error_code`, sanitized `herdr` topology,
coordinator-assigned `herdr_received_at`, and computed `stale`. Each session's
freshness is independent of the configured default: absent or unready default
Herdr does not make a fresh named session stale. Repeating an aggregate
inventory does not refresh unchanged sessions' observation receipt times.
The node also replaces expired cached ready snapshots with entity-free
`unavailable` / `observation_stale` states, even while other sessions refresh.
`ctl nodes -json` also exposes `sessions`, `sessions_ready`,
`sessions_received_at`, and `sessions_error_code`. Manager readiness
(`sessions.manage.v1`) permits bootstrap; it does not imply any particular
session or Herdr snapshot is ready. After ensure, wait for fresh ready selected
session topology before workspace/worktree or agent operations. Offline,
unsupported, unavailable, and replaced targets fail explicitly.

Ensure is the durable command `sessions.ensure.v1`, not an unjournaled launch.
Names must be portable lowercase names; platform-reserved names such as `con`,
`nul`, and `lpt1` are rejected. A successful ensure returns the **actual
64-lowercase-hex incarnation**. Re-ensure with a new key is a new operation:
it can observe the same running session or start a stopped one with a new
incarnation. Repeating the original key returns its historical receipt, not a
fresh liveness check. Preserve the same authenticated client identity, node,
name, key, and TTL after a disconnect or wait timeout; use `ctl command -id`
to inspect uncertainty. Do not change the key to escape an `INDETERMINATE`
startup outcome: startup may have happened. List and inspect before deciding
whether a genuinely new ensure is appropriate.

Workspace ensure, worktree create, and every agent operation (including
`agent read -follow`) accept `-session worker` and
`-session-incarnation <64hex>`. Preserve those values in exact retries along
with project/binding/worktree inputs and the original key. Workspace/worktree
results include `session_name` and `session_incarnation`; omitted pins are
resolved for admission without rewriting the original durable retry request.

```powershell
.\herdr-mesh.exe ctl ensure-workspace -server '<server>:50052' -node '<node-id>' `
  -project AgentFlow -session worker -session-incarnation '<incarnation>' -idempotency-key workspace-1
.\herdr-mesh.exe ctl agent get -server '<server>:50052' -node '<node-id>' `
  -session worker -agent w1:p1 -json
.\herdr-mesh.exe ctl agent read -server '<server>:50052' -node '<node-id>' `
  -session worker -session-incarnation '<incarnation>' -agent w1:p1 -follow -timeout 2m
```

Agent GET may discover by name without an incarnation and returns a fully
pinned named target. A terminal ID alone is insufficient for named effects:
the CLI still discovers if the session incarnation is missing. Supplying the
original terminal **and** session incarnation skips client discovery for
durable input retries, including when the original target is no longer live.
`read -follow` uses the same discovery rule and retains the resolved session
and terminal pins across polls.
Pins prevent intentional rebinding, not all races: Herdr offers no atomic
compare-and-swap (CAS) for expected session/terminal identity at input time.
The node refreshes the selected session before each mutation IPC and checks it
again before accepting success. Trusted local replacement between validation
and an effect remains possible. Legacy empty selectors retain their configured
default behavior; use an explicit session and its incarnation for restart
fencing.

Session discovery is bounded to 64 sessions per node, eight concurrent agent
queries, and 256 KiB of aggregate session inventory. An over-budget topology is
omitted with `session_capacity`, not returned as a complete tree. Session ensure
and interrupt have independent bounded command lanes, so pending startup or
read/wait queries do not globally serialize an interrupt. Reconnect cancels and
joins the old stream's observers and queries before starting replacements.

This integration is **headless-only**: it does not launch, attach, or automate
native terminal/GUI frontends. Ensuring a session is not dedicated-agent
launch/stop, and an input receipt is not proof of task completion.

## Headless existing-agent control

The standalone CLI can **get, read, wait, prompt, send explicit input, and
interrupt** an existing agent. It does not depend on MCP or an attached Herdr
terminal/window. Upgrade the coordinator and node together, then configure the
node's `-herdr-socket` or `-herdr-executable` for named sessions.
Existing Tailscale client/node/server role checks remain;
agent operations need no project allowlist. `ctl nodes -json` exposes
`agent_ready`, and the capability is `herdr.agent-control.v1`.

Use the pane ID shown in fleet inventory as `-agent`. Examples assume the
controller's normal enrolled state directory is unused by other processes:

```powershell
.\herdr-mesh.exe ctl agent get -server '<server>:50052' -node '<node-id>' -agent w1:p1 -json
.\herdr-mesh.exe ctl agent read -server '<server>:50052' -node '<node-id>' -agent w1:p1 -lines 100
.\herdr-mesh.exe ctl agent prompt -server '<server>:50052' -node '<node-id>' -agent w1:p1 `
  -prompt 'Describe the current project without changing files.'
.\herdr-mesh.exe ctl agent prompt -server '<server>:50052' -node '<node-id>' -agent w1:p1 `
  -prompt-file .\task.txt
Get-Content -Raw .\task.txt | .\herdr-mesh.exe ctl agent prompt `
  -server '<server>:50052' -node '<node-id>' -agent w1:p1 -prompt-file -
.\herdr-mesh.exe ctl agent wait -server '<server>:50052' -node '<node-id>' -agent w1:p1 `
  -until idle,done,blocked -wait-timeout 2m -timeout 3m
.\herdr-mesh.exe ctl agent input -server '<server>:50052' -node '<node-id>' -agent w1:p1 -key down -key enter
.\herdr-mesh.exe ctl agent interrupt -server '<server>:50052' -node '<node-id>' -agent w1:p1
```

Prompt files are read on the **client**, never opened remotely. Prompts are
bounded to 8,192 UTF-8 bytes. Read returns a sanitized recent terminal snapshot,
not a complete conversation: at most 1,000 lines and 65,536 bytes, with a
truncation indicator. `-json` produces one typed JSON object. Read/query output
is ephemeral, not persisted in command journals, fleet snapshots, or shared
memory. Submitted prompt text and explicit keys **are** retained in the
private coordinator/node command journals for exact retry comparison.

Before input/read/wait, the client discovers and pins the terminal and optional
provider session ID in the same authenticated connection. A replacement fails
explicitly rather than intentionally rebinding input to another agent. Herdr
does not provide an atomic expected-terminal input operation, so a trusted
local actor replacing a pane between preflight and input remains a local race.

Input commands produce **delivery receipts, not proof of task completion**.
Prompt refuses agents observed working or blocked; explicit input can answer
a provider prompt, but the mesh never automatically approves it. Interrupt is
currently verified only for Copilot and sends two Escape keys in one Herdr
request; unsupported providers fail explicitly. It is not force-kill, rollback,
or cancellation of independently detached work.

`wait` observes matching states (default `idle,done,blocked`), not a particular
turn or successful task outcome. It may return immediately for an already
matching state. Provider startup and status detection can transiently report
idle; inspect output before treating work as complete. Query timeouts are
bounded to five minutes; the overall `-timeout` must also allow enrollment and
connection time. Canceling a wait cancels only the query, never the agent task.

Input uses the existing durable command path and default ten-second TTL
(maximum thirty seconds). The client prints the idempotency key and pinned
target, and returns the command ID. For retries preserve the original
`-idempotency-key`, `-terminal`, optional `-agent-session`, input, TTL, and
any `-session` / `-session-incarnation` selection;
an explicit terminal (plus incarnation for a named session) bypasses discovery,
allowing the coordinator to return
the original receipt even when that agent is no longer available. Alternatively
use `ctl command -id <command-id>`. **Never generate a fresh key or change a
target to bypass an indeterminate outcome.** Inspect the original receipt and
the preserved target. The mesh never automatically retries uncertain input as
a new operation. Unknown input fences new prompt/input delivery to that terminal
across keys; it does not add the project-wide quarantine used by workspace/worktree
mutations. Read/query and explicitly pinned recovery controls remain separate.

These operations address existing agents on the configured default socket or
an explicitly selected named headless session. Dedicated-pane launch/stop is
available through `ctl agent start` and `ctl agent stop`, independently of
session ensure; see the [operator guide](docs/operator-guide.md) for complete
launch/task/stop examples and partial-outcome handling.
Project registration below uses the same authenticated client role, without
additional per-project actor allowlists.

Headless compatibility evidence and the opt-in live fixture are documented in
`docs\windows-live-validation.md`.

## Central project configuration

The coordinator is the durable authority for project configuration. Start the
server normally and the node with its existing identity/state and
`-herdr-socket` or `-herdr-executable`; either enables the node command journal
automatically, and the executable-only mode accepts project registration
before any session is running.
No workspace-policy files, actor IDs, or extra project grants are needed.
The existing Tailscale client/node/server role checks still apply.

Discover the mesh node instance ID with `ctl nodes`, then register an existing
Git checkout **on that node** from an unused enrolled client identity:

```powershell
.\herdr-mesh.exe ctl project register -server '<server>:50052' `
  -node '<mesh-node-instance-id>' -project AgentFlow `
  -path 'C:\dev\AgentFlow' -worktree-root 'C:\dev\AgentFlow-worktrees'
.\herdr-mesh.exe ctl projects -server '<server>:50052' -node '<mesh-node-instance-id>' -json
.\herdr-mesh.exe ctl project get -server '<server>:50052' `
  -node '<mesh-node-instance-id>' -project AgentFlow -json
```

Add your usual `-state-dir` and `-hostname` flags when reusing an enrolled client.
`ctl projects` without `-node` lists all registered bindings, following bounded
RPC pages automatically. Project IDs are 1-128 ASCII letters, digits, colons,
underscores, or hyphens. `-worktree-root` is optional: omission selects the
checkout's sibling `<checkout-basename>-worktrees`. An explicit root overrides
that default; include it again on updates to retain the override.
Paths are interpreted and validated by the target node, not opened by the client
or coordinator. Registration never clones or changes a checkout. The node may
create the one output-root directory after validating its existing parent;
unowned existing roots must be empty and disjoint from registered checkouts
and other roots. Successful ownership is retained in the implementation-private
node journal, not in operator-maintained JSON.

Registration persists desired configuration; it does **not** mean that the node
has applied it. Explicit project inspection shows desired and applied generations,
paths, `readiness`, `adoption_status`, and bounded validation error categories:

| Readiness | Meaning |
| --- | --- |
| `pending` | Waiting for the current node stream to validate and acknowledge the desired generation. |
| `offline` | No live node stream; registration remains durable for reconnect. |
| `applied` | The current stream has acknowledged the desired configuration. |
| `invalid` | Node-local validation rejected it; inspect the error category and correct registration. |
| `unsupported` | The connected node lacks managed-project support; upgrade/restart it with Herdr enabled. |

Desired and last-applied configurations can differ while pending or offline.
Do not treat a stored acknowledgement as live readiness after restart/reconnect.
Identical registration is idempotent; changed paths advance an internal
generation. New mutations require the current generation; already admitted
operations keep their original binding or are rejected before effects.
Workspace/worktree admission also requires a fresh ready Herdr baseline.
Project registration/get/list exposes configuration paths. Fleet observations,
dashboard output, and agent-query results also expose allowlisted display names
and reported working/checkout directories; mutation receipts and audit details
remain bounded and sanitized. Keep these inspection outputs and node-local paths out of shared
AI Core memory, logs intended for sharing, and committed files.

### Legacy policy migration

Upgrade the coordinator first, preserving its state directory, and **remove**
its old `-workspace-policy` flag. The server rejects that deprecated flag with
an actionable error instead of loading a competing authority. On an upgraded
node, the flag remains accepted only as deprecated one-time migration input:
start with the existing node policy and `-herdr-socket`, inspect central
`adoption_status` and readiness, then remove the flag on subsequent starts.
Existing central configuration takes precedence; editing the old file is not
an ongoing configuration mechanism. Resolve conflicts with `ctl project register`,
not coordinated edits to two policy files. The project ID remains the stable
logical key, never an inferred checkout path or workspace label.

Preserve both coordinator and node journals during migration. Historical
commands keep their original actor/key scopes and exact binding revisions;
old exact same-key retries still return their recorded operation. A new client
may inspect the known command ID but cannot reuse another actor's key to claim
its retry. Never reset journals or issue a fresh key to bypass uncertainty.

## Project-scoped workspace ensure

`ctl ensure-workspace` is the first real Herdr mutation. It ensures a workspace
for one **explicitly bound local Git checkout**: reuse the matching workspace, or
create one with `focus:false`. It does not create worktrees, clone repositories,
send prompts, inject commands/environment variables, rename or close workspaces.
Creating a Herdr workspace may start its normal local terminal; only bind
checkouts whose local startup behavior you trust.

Register the project centrally and wait for `applied` readiness as above.
Node bindings pin directory identity and recheck it before use. The coordinator
derives the effective binding revision; callers normally provide only node and
project. `-binding-revision` remains available as an exact override for advanced
use and historical retries.

From an unused enrolled client identity:

```powershell
.\herdr-mesh.exe ctl ensure-workspace `
  -server '<server-magic-dns-name>:50052' `
  -state-dir '<unused-enrolled-client-state-dir>' `
  -node '<mesh-node-instance-id>' `
  -project AgentFlow `
  -idempotency-key ensure-AgentFlow-1 `
  -ttl 30s `
  -json
```

Preserve any explicitly configured client hostname. Success returns
`workspace_ensure` with `project_id`, `binding_revision`, `workspace_id`, and
`created`. Neither paths nor Herdr labels, terminal data, or raw errors are
returned. Human output includes the same identifiers. `ctl command` retrieves
the durable outcome; retries must retain the same actor, key, target, request
options, and TTL. Reusing that key returns the original operation, not a new check after
someone closes its workspace.

`ctl nodes -json` exposes `workspace_ready` separately from probe `command_ready`.
Admission requires negotiated workspace support and a fresh ready Herdr baseline.
The coordinator rechecks the client's and node's WhoIs roles before dispatch;
the production node checks the actual coordinator peer again before its effect.
Both sides enforce the centrally configured project revision. Workspace execution is
serialized on the node without blocking heartbeats or observer traffic.

**Uncertainty is not retried.** A timeout, malformed response, or connection loss
after attempting creation can mean Herdr created the workspace. The command is
then `INDETERMINATE`; after interrupted intent, restart also remains indeterminate.
The node durably rejects new workspace or worktree keys for that project as
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

## Project-scoped worktree creation

`ctl create-worktree` creates one new linked Git worktree and its Herdr
workspace with `focus:false`. It requires a new branch, a new destination
name, and an exact commit already present in the bound repository. By default,
the branch is the destination name and the node resolves its checkout's exact
`HEAD` commit. `-branch` and `-base-commit <full-lowercase-sha>` preserve explicit
overrides; symbolic refs (including a literal `-base-commit HEAD`) are rejected.
It does not fetch, clone, adopt existing destinations, overwrite
branches, inject commands, or change the source checkout's branch.

Optionally configure `-worktree-root` through `ctl project register`.
Omitting it selects a sibling named `<checkout-basename>-worktrees`; the node
creates that directory if absent. Existing unowned roots must be empty.
Wait for the updated configuration
to be applied; policy edits, manually advanced revisions, and restarts are not
needed for registration changes. Worktree support requires the negotiated
`commands.worktree-create.v1` capability.

The output root is directory-identity pinned, cannot be a filesystem root, and
must not overlap any bound checkout or another output root, including aliases.
The destination is a direct child of that root. Names and branch names match
`[a-z0-9][a-z0-9_-]{0,63}`; Windows device names are rejected. Paths, slashes,
dots, flags, uppercase names, and arbitrary Git revision expressions are not
accepted. A destination that already exists, even an empty directory or symlink,
is rejected. An existing branch is rejected even when not checked out.

From an unused enrolled client identity:

```powershell
.\herdr-mesh.exe ctl create-worktree `
  -server '<server-magic-dns-name>:50052' `
  -state-dir '<unused-enrolled-client-state-dir>' `
  -node '<mesh-node-instance-id>' `
  -project AgentFlow `
  -name task-one `
  -idempotency-key create-task-one-1 `
  -json
```

The worktree CLI defaults to the maximum 30-second TTL; the other command
defaults remain 10 seconds. The node checks local Git preconditions, makes one
Herdr `worktree.create` call, then verifies the returned workspace/path and the
actual Git HEAD, branch, and common repository identity. Success includes
`worktree_create` with project, binding revision, workspace ID, name, branch,
and base commit. Paths, labels, and raw local diagnostics remain private.
`ctl nodes -json` exposes `worktree_ready`, requiring fresh Herdr observation
as well as negotiated support. The same client-role/revision checks, peer
reauthorization, serialized node worker, and durable receipts apply.

Repeat an interrupted CLI invocation only with the **same identity, key, target,
arguments, and TTL**, or inspect it with `ctl command`. Every argument is bound
to that key. When defaults were omitted, keep them omitted on retry; the existing
command retains its derived binding, and its success receipt reports the exact
resolved base. A recorded attempt is never reexecuted against a later checkout
HEAD. Explicit historical revision/branch/base overrides must
remain exactly the same. A timeout does not undo or necessarily stop an already-started
Herdr operation. Missing or inconsistent creation metadata, failed postchecks,
lost replies, and interrupted execution produce `INDETERMINATE`, with no
automatic retry, rollback, branch deletion, or directory cleanup. Uncertainty
quarantines **both workspace and worktree mutations for the project**, including
new keys and after both journals restart. Reconciliation tooling is still
pending; do not clear journals or rebind projects to bypass this protection.

Only bind trusted repositories and use a trusted local Git installation.
Herdr's normal Git hooks, filters, and terminal startup behavior may run; this
is not a sandbox. External local operations can race the final path/branch
checks, so do not concurrently replace checkout/root directories or create
the same target. This does not claim atomic filesystem confinement against
hostile local processes or exactly-once Herdr execution.

Windows integration coverage uses isolated temporary Git repositories, fake
local Herdr IPC, the real node runtime, and both SQLite journals. Actual Herdr
worktree mutation on the live mesh and the full two-host disruption gate
remain unvalidated. CLI and MCP expose these operations without changing their
targeting, retry, or uncertainty guarantees.

## Archived developer experiment

The disposable transport spike is retained under
[`experiments/tsnet-spike`](experiments/tsnet-spike/README.md), outside production
entrypoints. It is not shipped and is not an alternative operator command.
