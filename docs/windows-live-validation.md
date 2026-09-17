# Windows two-node operational acceptance

Test the installed **`herdr-mesh`** CLI end to end, not a spike, helper script,
or direct native Herdr command. This runbook covers the complete current mesh:
central project configuration, headless sessions, workspaces/worktrees, agent
launch/task/control/stop, observation, and recovery. A passing local fixture or
cross-build is not a pass for this real two-host gate.

## Names, defaults, and prerequisites

Use two prepared Windows machines: **A** runs the coordinator and execution node
A; **B** runs execution node B and the controller. Mesh roles run in separate
processes, but no Herdr terminal/frontend should be attached.

Examples use these illustrative values directly, with no variable setup block:

| Value | Meaning |
|---|---|
| `herdr-mesh-server:50052` | Default requested coordinator hostname and RPC port; use its actual assigned full MagicDNS name if needed |
| `smoke` | Operator-chosen project ID and named Herdr session |
| `C:\src\mesh-demo` | Existing disposable Git checkout on each node; substitute that node's real checkout path |
| `smoke-a-01`, `smoke-b-01` | Worktree/agent names for this acceptance run |
| `<node-a-id>`, `<node-b-id>` | Stable mesh instance IDs returned by `ctl nodes`, not hostnames or invented aliases |
| `<session-incarnation>`, `<workspace-id>`, `<pane-id>`, etc. | Exact returned identities; paste the values into the later commands |

Keep opaque IDs explicit: node/session/terminal pinning prevents a command from
silently acting on a replacement. Named projects, sessions, worktrees and retry
keys simplify the rest. Choose the `01` run suffix once for a new run; keep every
key and payload unchanged for an exact retry. Never increment a suffix to bypass
an unknown outcome.

Install the same release's single executable on `PATH` on both hosts. Herdr
(native protocol 18), Git, a supported provider such as Copilot, and provider
authentication must already be available to the node account. Use disposable
checkouts and prepared test accounts/hosts. If their default mesh role state is
already in use, explicitly select a separate private `-state-dir` and retain it
for every restart; do not delete or copy existing state to make the defaults work.

Default server, node, doctor, controller and dashboard state directories are
separate. On Windows they are under `%APPDATA%\herdr-mesh`. Ordinary commands
below omit redundant hostname, state, tag and timeout flags. A running watch,
follow, or wait owns its controller state: cancel that observation before the
next command, or use a separately enrolled controller/MCP process.

Enrollment secrets are the only required environment setup. Supply them through
trusted private secret handling, never inline command arguments or transcripts:

| Role | Default secret environment variable | Required Tailscale tag |
|---|---|---|
| Coordinator | `TS_AUTHKEY_SERVER` | `tag:herdr-mesh-server` |
| Execution node | `TS_AUTHKEY_NODE` | `tag:herdr-mesh-node` |
| Doctor/controller/dashboard/MCP | `TS_AUTHKEY_CLIENT` | `tag:herdr-mesh-client` |

Each independently enrolled process state needs its own key. Existing enrolled
states need no new key. Never distribute a server/client credential to a node or
copy enrolled identities between machines. Clear consumed enrollment variables,
remove private consumed key artifacts, and revoke unused keys. Provider
authentication and permission decisions are separate; the mesh never approves
dialogs automatically.

## 1. Prepare the tailnet if necessary

Supply `TAILSCALE_API_TOKEN` privately. This must be an API access token, not a
device enrollment auth key. Preview is the default:

```powershell
herdr-mesh setup tailnet -tailnet example.com -output-directory C:\private\mesh-preview -json
herdr-mesh setup tailnet -tailnet example.com -output-directory C:\private\mesh-enrollment -keys-per-role 4 -apply -json
```

Run the second command only after reviewing the first proposal. The explicit
apply directory must be different; proposals/key files are not overwritten.
Four keys per role accommodate the main roles and optional isolated negative/
concurrency tests; reduce that count if fewer identities are needed. PowerShell
is an internal setup prerequisite, not another shipped product entrypoint.

The merge preserves unrelated policy, uses ETag protection, and adds role access
to TCP `50052`. Existing wildcard ACLs remain: added grants alone do not establish
network isolation. Application peer-role checks remain necessary. JSON policy
serialization can remove HuJSON comments/formatting; preserve the private original
backup. See [Tailnet setup](tailnet-setup.md) for key artifacts and uncertain-apply
recovery. Do not automatically retry an unknown apply.

## 2. Start the roles

On **A**, start the coordinator:

```powershell
herdr-mesh version
herdr-mesh server
```

Leave it running. In another process on **A**, and separately on **B**, start a
node after supplying that machine's own node-role enrollment key:

```powershell
herdr-mesh node -server herdr-mesh-server:50052 -herdr-executable herdr
```

`herdr` is resolved from the node account's PATH. No preexisting default Herdr
session, socket variable, or attached Herdr frontend is required.

From the controller on **B**:

```powershell
herdr-mesh doctor -server herdr-mesh-server:50052
herdr-mesh ctl nodes -server herdr-mesh-server:50052 -json
```

Retain both returned node instance IDs. **Pass:** both execution nodes are
connected and advertise session-management readiness. A node may have no ready
Herdr session yet; session ensure is the next step. Doctor should report the
expected role, compatible protocol, and actual identity/connectivity.

## 3. Register the project centrally on both nodes

```powershell
herdr-mesh ctl project register -server herdr-mesh-server:50052 -node '<node-a-id>' -project smoke -path C:\src\mesh-demo
herdr-mesh ctl project register -server herdr-mesh-server:50052 -node '<node-b-id>' -project smoke -path C:\src\mesh-demo
herdr-mesh ctl projects -server herdr-mesh-server:50052 -json
```

**Pass:** both mappings converge to valid applied configuration. Paths refer to
the selected node, not the controller. Revisions and the worktree-root default
are managed internally; no policy JSON, revision variables, project grants, or
external catalog is needed.

## 4. Ensure named sessions and isolated workspaces

```powershell
herdr-mesh ctl session ensure -server herdr-mesh-server:50052 -node '<node-a-id>' -name smoke -key session-a-01 -json
herdr-mesh ctl session ensure -server herdr-mesh-server:50052 -node '<node-b-id>' -name smoke -key session-b-01 -json
herdr-mesh ctl sessions -server herdr-mesh-server:50052 -node '<node-a-id>' -json
herdr-mesh ctl sessions -server herdr-mesh-server:50052 -node '<node-b-id>' -json
herdr-mesh ctl ensure-workspace -server herdr-mesh-server:50052 -node '<node-a-id>' -project smoke -session smoke -idempotency-key workspace-a-01 -json
herdr-mesh ctl ensure-workspace -server herdr-mesh-server:50052 -node '<node-b-id>' -project smoke -session smoke -idempotency-key workspace-b-01 -json
herdr-mesh ctl create-worktree -server herdr-mesh-server:50052 -node '<node-a-id>' -project smoke -session smoke -name smoke-a-01 -idempotency-key tree-a-01 -json
herdr-mesh ctl create-worktree -server herdr-mesh-server:50052 -node '<node-b-id>' -project smoke -session smoke -name smoke-b-01 -idempotency-key tree-b-01 -json
```

**Pass:** sessions are `ready` with distinct retained incarnations; workspaces
and linked worktrees have confirmed receipts. The checkout needs a commit before
worktree creation. Branch/base/root defaults avoid additional configuration.

Replay an exact completed command with the same key to verify receipt reuse,
not re-execution. To test native workspace reuse separately, after a confirmed
ensure submit a genuinely new ensure key for that same checkout: it should
return the existing workspace with `created:false`. Never do this after an
unknown outcome. Preserve the worktree workspace IDs for agent start.

## 5. Launch one headless agent/task per node

Choose fresh response markers before the first submission; the example `01`
markers are illustrative, not reusable proof from a prior run.

```powershell
herdr-mesh ctl agent start -server herdr-mesh-server:50052 `
  -node '<node-a-id>' -project smoke -session smoke -session-incarnation '<session-a-incarnation>' `
  -workspace '<worktree-a-workspace-id>' -provider copilot -name smoke-a-01 `
  -prompt 'Reply with exactly MESH-SMOKE-A-01' -idempotency-key start-a-01 -json
herdr-mesh ctl agent start -server herdr-mesh-server:50052 `
  -node '<node-b-id>' -project smoke -session smoke -session-incarnation '<session-b-incarnation>' `
  -workspace '<worktree-b-workspace-id>' -provider copilot -name smoke-b-01 `
  -prompt 'Reply with exactly MESH-SMOKE-B-01' -idempotency-key start-b-01 -json
```

These use the defaults: 10s dispatch TTL, 30s startup readiness budget, and 2m
client wait. Increase only the budget actually needed. A client timeout does not
cancel/restart the remote provider. For a larger task use `-prompt-file task.txt`
(controller-local UTF-8, at most 8 KiB) instead of `-prompt`.

Retain each full handle and command receipt, including partial handles on error.
Pane, launch, prompt and stop outcomes are independent. **Pass:** a dedicated
pane is created without focus theft and the provider emits the fresh requested
assistant response. Prompt echo, input acknowledgment, `idle`, and `done` alone
are not semantic task-success evidence. Record provider permission/authentication
blocks as blocked, not passed; never attach a frontend as a workaround.

## 6. Observe, follow up, interrupt and stop

Use node A's preserved handle below, then repeat for B using B's own IDs and
distinct operation keys.

```powershell
herdr-mesh ctl agents -server herdr-mesh-server:50052 -project smoke -provider copilot -json
herdr-mesh ctl agent get -server herdr-mesh-server:50052 -node '<node-a-id>' -session smoke -agent '<pane-a-id>' -json
herdr-mesh ctl agent read -server herdr-mesh-server:50052 -node '<node-a-id>' -session smoke -agent '<pane-a-id>' -follow -timeout 5m -json
```

Cancel follow after inspecting the response, freeing the controller state before
the next command. Refresh and retain the identity; do not substitute a replacement.

```powershell
herdr-mesh ctl agent prompt -server herdr-mesh-server:50052 -node '<node-a-id>' -session smoke -agent '<pane-a-id>' -prompt 'Reply with exactly MESH-FOLLOWUP-A-01' -idempotency-key followup-a-01 -json
herdr-mesh ctl agent wait -server herdr-mesh-server:50052 -node '<node-a-id>' -session smoke -agent '<pane-a-id>' -json
herdr-mesh ctl agent read -server herdr-mesh-server:50052 -node '<node-a-id>' -session smoke -agent '<pane-a-id>' -json
herdr-mesh ctl agent interrupt -server herdr-mesh-server:50052 -node '<node-a-id>' -session smoke -agent '<pane-a-id>' -idempotency-key interrupt-a-01 -json
herdr-mesh ctl agent stop -server herdr-mesh-server:50052 `
  -node '<node-a-id>' -session smoke -session-incarnation '<session-a-incarnation>' `
  -workspace '<worktree-a-workspace-id>' -tab '<tab-a-id>' -agent '<pane-a-id>' -terminal '<terminal-a-id>' `
  -provider copilot -idempotency-key stop-a-01 -json
herdr-mesh ctl command -server herdr-mesh-server:50052 -id '<command-id>' -json
```

Interrupt an actual running disposable task to assess cancellation; an idle
interrupt alone is not that proof. Explicit key input uses `ctl agent input
-key esc` with the same selected target, only when that is the intended response
to the observed screen. Never guess or automatically approve a provider choice.
For stop, also supply `-agent-session` if the preserved handle reports that ID.
**Pass:** only the selected pane/terminal stops, other panes and the worktree/
workspace remain, and receipts accurately retain any uncertainty. The native
incarnation is mandatory for stop, even for an omitted configured-default name.

## 7. Observation and operational surfaces

Run these separately, or use their separate enrolled role states:

```powershell
herdr-mesh ctl nodes -server herdr-mesh-server:50052 -watch -timeout 10m -json
herdr-mesh dashboard -server herdr-mesh-server:50052
herdr-mesh mcp -server herdr-mesh-server:50052
```

Watch emits full replacement snapshots and explicit gaps. A reconnect starts
with a full snapshot, not event replay. Retained inventory is not fresh during
a gap. The standalone dashboard is at `http://127.0.0.1:8787`; no system
Tailscale client is needed for this loopback path. MCP uses stdio through the
operator's MCP client; all 20 tools use the same control services and pinned
selectors. Concurrent MCP tools share a connection and reserve stop/interrupt
capacity. Separate CLI processes need independent enrollment/state.

For optional prepared-endpoint installation use [bootstrap](node-bootstrap.md).
For a coordinator-hosted browser use [hosted dashboard](coordinator-dashboard.md),
whose browser peer needs its own tailnet connectivity and client role. For stopped
roles use [offline maintenance](journal-maintenance.md). These are modes of the
same CLI, not separate product scripts.

## Required acceptance matrix

| Test | Procedure and pass condition |
|---|---|
| Two-node headless flow | Complete steps 2-6 on both execution nodes with no Herdr frontend; retain actual task-response and preservation evidence |
| Role rejection | Use the isolated wrong-role command below; expect server `PermissionDenied`, not an enrollment failure |
| Central offline convergence | Disconnect B, change desired configuration, reconnect B; it applies the configuration without a copied file. Mutations are not queued offline |
| Session/workspace reuse | Exact retries retain receipts; a new confirmed-safe ensure reuses a healthy session/workspace rather than duplicating it |
| Replacement fencing | A restart/replaced session or terminal must reject an old pinned handle; it must not silently retarget |
| Unknown outcomes | On lost results inspect the original receipt/target. New keys, actors or aliases must not bypass uncertainty; do not reset journals |
| Concurrent control | With a separate enrolled controller or one MCP connection, stop/interrupt remains responsive while start/wait is pending |
| Observation gaps | Disconnect/reconnect during watch/follow; report stale/gap state, retain target pins, and resume only authoritative current snapshots |
| Server/node restart | Restart the exact roles with unchanged state/arguments; mesh and Tailscale IDs remain stable and nodes reconnect |
| Node before server | Start a node first; it reconnects without a tight retry loop after the coordinator starts |
| Network and sleep/wake | Change the expected network and suspend/resume where practical; recover without re-enrollment |
| Direct address | Substitute the coordinator's actual tailnet IP in doctor; retain compatible-protocol evidence |
| Hostname collision | Use an independently enrolled test server state with the same requested hostname; record the assigned names without retargeting the original controller |
| Disruption loop | Repeat server restart, node restart and network interruption three times over at least ten minutes; account for every disconnect |
| Offline preservation | Stop the exact role, inspect/back up/verify its full state; retain uncertainty and identity. Never back up a live SQLite file alone |

For role rejection, use a spare node-role key and an isolated test client state;
do not use or copy either execution node's live state:

```powershell
herdr-mesh ctl server-info -server herdr-mesh-server:50052 `
  -auth-key-env TS_AUTHKEY_NODE -tags tag:herdr-mesh-node -hostname mesh-wrong-role `
  -state-dir "$env:APPDATA\herdr-mesh\wrong-role"
```

## Evidence and limitations

Keep version/commit, host versions, assigned identities, private receipts,
response markers, restart timings, disruptions, and the final go/no-go record
in private operational records, not shared AI Core memory or this repository.
Never include enrollment/API secrets or raw private transcripts in PR evidence.
This runbook records required procedures, not a claim they have passed.

Raw Windows pipes without native incarnation markers retain legacy query/control
but cannot lifecycle-pin; the normal `-herdr-executable` path supports native
session management. Protocol 18 lacks atomic expected-terminal CAS, so a final
race with another trusted local client remains despite immediate checks.
Provider startup/status and interrupt behavior are provider-specific; readiness
is not task success. Real other-platform runtime acceptance and signing are
separate from cross-builds.

## Developer-only automated checks

These repository commands are not prerequisites or alternative operator CLIs:

```text
go test ./src/... ./experiments/... -timeout=180s
go vet ./src/...
node --test src\internal\dashboard\web\model.test.mjs
pwsh -NoProfile -File .\scripts\test-configure-tailnet.ps1
pwsh -NoProfile -File .\scripts\test-bootstrap-node.ps1
```

The native `TestLiveProjectObservation` fixture is opt-in through
`HERDR_MESH_OBSERVATION_LIVE=1`: it creates a disposable headless session and
checkout, exercises ensure/reuse/worktree projection without a provider, and
stops/deletes only its owned session. Provider-spending fixtures are separately
opt-in and must only target disposable agents. Automated local IPC/SQLite/gRPC
checks do not replace the real two-host matrix above.
