# Herdr Mesh operator guide

## One command

Install the platform release's single public executable on `PATH`. Its command
name is **`herdr-mesh`** on every platform; Windows stores it as
`herdr-mesh.exe`. Every product command in this guide uses that entrypoint.
Different process roles require separate state directories, not different binaries.
Each role has its own default private state; ordinary single-role launches do
not need `-state-dir` or `-hostname`. Use explicit overrides only for additional
instances, and preserve those overrides across restarts. In examples,
`<coordinator>` is normally `herdr-mesh-server`; substitute the actual assigned
full MagicDNS name if needed. Node IDs are returned identifiers, not hostnames.
For a concrete two-machine testing sequence, see
[Windows operational acceptance](windows-live-validation.md).

```powershell
herdr-mesh help
herdr-mesh version
```

This guide describes implemented source capabilities, not a completed deployment.
Setup, bootstrap, durable agent launch/stop, observation, and MCP are modes of
this CLI. Real two-node, provider, and platform acceptance are separate release
gates. Do not substitute a spike binary, milestone build name, or standalone
product script for a missing public command.

Herdr, Git, an existing checkout, and a supported authenticated provider remain
execution-node prerequisites. Herdr is the underlying headless runtime, not a
second mesh CLI the operator must use for routine session control. Provider
authentication and permission decisions are not performed implicitly.

## Prepare the tailnet

If role setup is needed, supply a Tailscale API access token through your
private environment, then preview the policy changes:

```powershell
herdr-mesh setup tailnet -tailnet example.com -output-directory C:\private\mesh-preview -json
```

Preview makes no remote policy/key changes. Review the private proposal before
an explicit apply with a separate artifact directory:

```powershell
herdr-mesh setup tailnet -tailnet example.com -output-directory C:\private\mesh-applied -apply -json
```

This is embedded in the executable; no source checkout or separately distributed
setup script is needed. PowerShell remains an internal runtime prerequisite.
Existing policy rules are preserved, including broad wildcard rules; adding role
grants does not remove those rules or establish network isolation by itself.
See [Tailnet setup](tailnet-setup.md) for artifact privacy and uncertain-apply
recovery. API access tokens and generated device enrollment keys are different.
Replace `example.com` with your tailnet; `TAILSCALE_API_TOKEN` is the default
API-token environment variable, so no flag is needed for it.

## Optional remote Windows bootstrap

For a prepared Windows endpoint, stage a trusted Windows release through an
existing SSH Host alias with strict host-key checking:

```powershell
herdr-mesh bootstrap -ssh-host '<trusted-ssh-alias>' `
  -archive '<local-windows-release.zip>' -sha256 '<trusted-archive-sha256>' -architecture amd64 `
  -install-dir '<new-target-install-directory>' -state-dir '<existing-private-node-state>' `
  -server '<coordinator>:50052' -herdr-executable '<existing-target-herdr.exe>' -json
```

This requires Windows, PowerShell 7.4+, OpenSSH, existing SSH authentication and
host trust, and a prepared PowerShell remoting endpoint. No checkout or standalone
bootstrap script is required. It does not create credentials, a service, or a
Scheduled Task. Without `-herdr-executable`, staging configures a transport-only
node, not the full headless project/session workflow.

The default result is staged, not running. To use a previously configured exact
Scheduled Task, repeat the same install inputs with explicit start options:

```powershell
herdr-mesh bootstrap -ssh-host '<trusted-ssh-alias>' `
  -archive '<local-windows-release.zip>' -sha256 '<trusted-archive-sha256>' -architecture amd64 `
  -install-dir '<same-target-install-directory>' -state-dir '<existing-private-node-state>' `
  -server '<coordinator>:50052' -herdr-executable '<existing-target-herdr.exe>' `
  -existing-task '<full-existing-task-name>' -start -json
```

The runner must already match the executable and fixed arguments. A running
runner is not verified mesh readiness; inspect it through `ctl nodes` afterward.
An unknown start is not automatically retried. See [Bootstrap](node-bootstrap.md)
for endpoint preparation, exact-match requirements, and uncertainty handling.

## Start the mesh roles

Prepare the existing tailnet's server, node, and client roles and supply their
respective enrollment secrets through a private environment. The standard
variables are `TS_AUTHKEY_SERVER`, `TS_AUTHKEY_NODE`, and `TS_AUTHKEY_CLIENT`.
Never put keys in command arguments, shared project configuration, or transcripts.
An already-enrolled role reuses its persistent state.

On the coordinator machine:

```powershell
herdr-mesh server
```

On each execution machine, use its default node state and Herdr from PATH:

```powershell
herdr-mesh node -server '<coordinator>:50052' -herdr-executable herdr
```

The processes remain running independently of operator commands. The
`-herdr-executable` option enables named headless sessions without requiring a
preexisting default session or a visible Herdr frontend.

From the controller:

```powershell
herdr-mesh doctor -server '<coordinator>:50052'
herdr-mesh ctl nodes -server '<coordinator>:50052' -json
herdr-mesh ctl nodes -server '<coordinator>:50052' -watch -timeout 10m -json
```

Retain the returned stable mesh node IDs locally. A running process, successful
enrollment, and an execution-ready node are distinct conditions.

Watch emits JSONL `snapshot` events whose `fleet` object completely replaces the
previous inventory. `gap` events mark a disconnected/unavailable feed; retained
inventory is not fresh. Reconnection starts with a full snapshot, not event replay.
Each stream is bounded to five minutes and renewed within the overall timeout.
Canceling or reaching that timeout stops observation only; it changes no nodes or
agents. Role loss and capacity errors stop the subscription rather than retrying
indefinitely.

A long-running watch, follow, or client wait owns that controller state
directory. For another concurrent CLI process, use its own `-state-dir` and
`-hostname`, enrolled independently with the client role; never copy an enrolled
identity. Alternatively cancel the current observation/client wait first and
then reuse its state. Canceling it does not stop the remote agent.

## Configure a project centrally

Register the existing checkout on the selected node. Paths belong to that node,
not the controller. Repeat for the other node with its own checkout mapping.

```powershell
herdr-mesh ctl project register -server '<coordinator>:50052' `
  -node '<node-id>' -project example-app -path C:\src\example-app
herdr-mesh ctl project get -server '<coordinator>:50052' `
  -node '<node-id>' -project example-app -json
herdr-mesh ctl projects -server '<coordinator>:50052' -json
```

Replace the example checkout path with the actual existing path on each node.
Wait for a valid applied configuration before execution. An offline node can
receive configuration after reconnect, but mutations are not queued for later
offline execution. No copied project-policy files, project grants, or external
catalog are required.

## Ensure a named headless session and workspace

```powershell
herdr-mesh ctl session ensure -server '<coordinator>:50052' `
  -node '<node-id>' -name dev -key ensure-dev-01 -json
herdr-mesh ctl sessions -server '<coordinator>:50052' -node '<node-id>' -json
herdr-mesh ctl ensure-workspace -server '<coordinator>:50052' `
  -node '<node-id>' -project example-app -session dev -json
```

A healthy session is reused; a stopped one can be started again with a new
incarnation. Commands and retries retain resolved session identity rather than
following a replacement silently.

Workspace ensure opens the existing Git checkout as a repository-aware headless
workspace and reuses it on subsequent calls; it does not create a Git worktree.
Project association is based on native repository identity and the applied central
binding, not the workspace label or directory name. Plain shell workspaces without
native repository metadata remain unassociated.

For an isolated worktree:

```powershell
herdr-mesh ctl create-worktree -server '<coordinator>:50052' `
  -node '<node-id>' -project example-app -session dev -name task-a -json
```

Use the returned workspace/command identities. Branch and base defaults are
resolved safely by the node; no general remote shell is exposed.

## Start an agent and submit a task

Use the project, workspace, and session incarnation returned by the preceding
operations. The start creates a dedicated pane without stealing focus:

```powershell
herdr-mesh ctl agent start -server '<coordinator>:50052' `
  -node '<node-id>' -project example-app -workspace '<workspace-id>' `
  -session dev -session-incarnation '<session-incarnation>' `
  -provider copilot -name task-a -prompt-file task.txt `
  -idempotency-key start-task-a-01 -json
```

Omit `-prompt-file` to launch without an initial task. Prompt files are local to
the controller; create `task.txt` with the intended task first, use
`-prompt 'Summarize this repository'` for a short inline task, or use
`-prompt-file -` to read stdin. Prompts are bounded to 8 KiB of
UTF-8. Provider installation, authentication, and permission decisions remain
explicit prerequisites, not actions the mesh performs silently.

Retain the receipt and full handle, including session incarnation, workspace,
tab, pane, terminal, and provider identity. Pane creation, provider launch,
prompt submission, and stop have independent durable outcomes. A failed command
can still have confirmed earlier effects; an unknown stage might have happened.
Do not discard a partial handle, relaunch under a new key, or resubmit uncertain
input. Inspect the original command receipt.

Dispatch TTL (default 10s, maximum 30s), startup readiness budget (default 30s,
3,001ms through 5m), and client wait timeout (default 2m) are separate. Expiring the client
wait does not restart or cancel the provider. Accepted input is not proof of
semantic task completion.

Lifecycle pinning requires a native session incarnation, normally available
through a node configured with `-herdr-executable`. Legacy raw Windows pipes
without incarnation markers retain existing-agent query/control compatibility,
but cannot support durable lifecycle pinning. Native protocol 18 also lacks an
atomic expected-terminal check-and-mutate operation: the adapter rechecks before
effects, but a final race with another trusted local client remains.

## Observe and control an agent

Use observed inventory to select its pane; a live get refreshes pinned terminal,
provider-session, and Herdr-session identities.

```powershell
herdr-mesh ctl agents -server '<coordinator>:50052' -json
herdr-mesh ctl agents -server '<coordinator>:50052' -node '<node-id>' `
  -project example-app -workspace '<workspace-id>' -provider copilot -ready true -json
herdr-mesh ctl agent get -server '<coordinator>:50052' `
  -node '<node-id>' -session dev -agent '<pane-id>' -json
herdr-mesh ctl agent read -server '<coordinator>:50052' `
  -node '<node-id>' -session dev -agent '<pane-id>' -follow -timeout 5m -json
herdr-mesh ctl agent prompt -server '<coordinator>:50052' `
  -node '<node-id>' -session dev -agent '<pane-id>' `
  -prompt 'Summarize your progress' -idempotency-key progress-task-a-01 -json
herdr-mesh ctl agent wait -server '<coordinator>:50052' `
  -node '<node-id>' -session dev -agent '<pane-id>' -timeout 5m -json
herdr-mesh ctl agent input -server '<coordinator>:50052' `
  -node '<node-id>' -session dev -agent '<pane-id>' -key esc -json
herdr-mesh ctl agent interrupt -server '<coordinator>:50052' `
  -node '<node-id>' -session dev -agent '<pane-id>' -json
```

`ctl agents` is observed fleet inventory, not a live provider query. It retains
per-node/session `sources` even when no agents match. Agent rows contain their
exact source scope, target, workspace/tab, provider, project, and optional
readiness. `-ready` accepts `any`, `true`, `false`, or `unknown`; matching an old
readiness value does not make a stale row ready for execution. Refresh the
selected identity with `ctl agent get` before mutation. A missing configured-
default incarnation is unknown, not permission to invent a native session name.
Configured-default and named routes are kept separate when the protocol does not
prove they are the same endpoint; a session literally named `default` must not
hide a different configured endpoint.

Input is explicit; the mesh never approves dialogs for you. Interrupt support
is provider-specific. Canceling a read/follow/wait only ends observation, not
the agent's task. Following emits bounded JSONL snapshots and explicit gaps,
and stops rather than retargeting a replaced session or terminal.

Retain each command's original key, payload, timing, and resolved target for
an exact retry. Example keys ending in `01` name one operation, not a key to
reuse for different input; choose another name only for a genuinely new
operation, never to bypass uncertainty. Inspect a receipt directly:

```powershell
herdr-mesh ctl command -server '<coordinator>:50052' -id '<command-id>' -json
```

Acknowledged input, observed `done`, and semantic task success are different.
Never generate a new key or change the target to bypass an unknown outcome.

## Stop the selected agent

Use the preserved handle, not a newly discovered replacement:

```powershell
herdr-mesh ctl agent stop -server '<coordinator>:50052' `
  -node '<node-id>' -session dev -session-incarnation '<session-incarnation>' `
  -workspace '<workspace-id>' -tab '<tab-id>' -agent '<pane-id>' -terminal '<terminal-id>' `
  -provider copilot -idempotency-key stop-task-a-01 -json
```

If the handle includes an opaque provider-session ID, also pass
`-agent-session` with that exact value. The native incarnation is required even
when `-session` is omitted to select the configured default. Stop verifies the
selected pane/terminal, refuses to remove the last workspace pane, and preserves
the workspace/worktree. It does not claim graceful provider shutdown or delete
branches/files. Stop and interrupt have execution capacity independent of a
long-running start or wait.

## Dashboard, MCP, and maintenance

These are additional modes of the same executable:

```powershell
herdr-mesh dashboard -server '<coordinator>:50052'
herdr-mesh mcp -server '<coordinator>:50052'
herdr-mesh maintenance inspect -role node -state-dir '<stopped-node-state>' -json
```

Dashboard and concurrent MCP/controller processes need independent client state;
they cannot share a live tsnet identity directory. All 20 MCP tools map through
the same control services, including agent start/stop, filtered discovery, and
partial receipts. Concurrent tools within one MCP process share its connection;
interrupt/stop retain reserved capacity when ordinary calls are busy.

The coordinator can instead host the dashboard with its own identity; see
[Coordinator-hosted dashboard](coordinator-dashboard.md). For consistent offline
backups and recovery limits, see [Journal maintenance](journal-maintenance.md).
Maintenance requires stopped roles, preserves all retry state, and does not
prune, force outcomes, or automatically restore old journals.
