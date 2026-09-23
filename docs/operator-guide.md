# Herdr Mesh: connect two computers and run a task

Use one command, **`herdr-mesh`**, on both computers. Each computer has one
background mesh connection. The dashboard, diagnostics, and operator commands
share that connection; they do not need separate Tailscale setup.

The examples name the computers **desktop** and **laptop**. Desktop coordinates
the mesh and can run agents. Laptop can run agents too. Either computer can
control the mesh.

## Before you start

On both computers, install the same build of `herdr-mesh` (`herdr-mesh.exe` on
Windows), plus Herdr and Git on PATH. Install and sign in to the provider you
want to use; the default is Copilot. A Herdr terminal window does not need to
be open.

Use a mesh build supporting your installed Herdr's native protocol. This build
supports protocols **18, 20 and 22**, including **Herdr 0.8.2 and 0.9.1**. Other native protocol
versions are rejected rather than assumed compatible.

You need a Tailscale account and permission to configure its private network.
Tailscale calls that network a **tailnet**. Use its name instead of `example.com`
below. A separate system Tailscale installation is not required for the embedded
mesh connection.

Managed setup and background startup support Windows, Linux, and macOS. The
process survives closing the launching terminal. Mesh uses no Windows registry
and installs no sign-in autostart, boot service, or scheduled task. After a
reboot or shutdown, run `herdr-mesh start` explicitly.

### Where this installation lives

Configuration, databases, journals, tsnet identity, and logs are stored in
**`herdr-mesh-state` beside the executable**, not beside your current working
directory or in AppData. Choose a writable, private local installation directory.
Do not store or sync tsnet state to multiple computers: each computer must enroll
its own identity. A OneDrive copy of the executable is a delivery artifact;
copy it into a local, non-synced installation directory before running it.

Normal startup and policy preview/apply accept parent-directory links, including Herdr's installer-managed
`bin` junction. The state directory itself and its private files must remain
ordinary directories/files. Destructive cleanup and journal maintenance retain
stricter path checks; use a physical (non-aliased) state path for those operations.

To select a different state directory, put the optional global `--state-dir`
**before** the command and use an absolute path:

```powershell
herdr-mesh --state-dir C:\private\mesh-state start
```

The same global override applies to `init`, `join`, `shutdown`, `status`, `nodes`,
`dashboard`, `mcp`, `project`, and `agent`. Use it consistently for the selected
installation; the examples below use the executable-relative default.

## 1. Create the mesh on desktop

```powershell
herdr-mesh init --tailnet example.com --name desktop
```

`--name` is optional on both `init` and `join`. Without it, mesh uses the OS
hostname normalized to lowercase letters, digits, and hyphens, adding a prefix
if needed to start with a letter and limiting the result to 40 characters.
The example overrides the coordinator name to `desktop`; use `--name` when you
want a different label from your normalized hostname.

Follow the prompts. First-time network configuration needs a **Tailscale API
access token**: a secret that lets setup configure the network on your behalf.
The command explains where to obtain it and accepts a hidden paste. It is not
an agent/provider credential, and you do not put it in command arguments or
environment-variable setup blocks. Setup does not retain it for normal use.

Review the proposed network changes before approving them. Existing unrelated
rules are preserved. If the network already contains broad access rules, setup
does not silently remove them.

Complete the displayed Tailscale browser sign-in when requested. If you run the
command remotely without a browser, open the displayed URL in your browser.
If your tailnet requires device approval, complete that approval too.

When ready, the command prints the exact join command for another computer.
It uses the coordinator's **actual full MagicDNS name**, including its tailnet
suffix. Do not construct that address yourself.

## 2. Join from laptop

Copy the command printed on desktop. For example, if desktop printed the
following address:

```powershell
herdr-mesh join --server herdr-mesh-desktop.example.ts.net
```

The printed command has no `--name`: it uses this computer's normalized hostname.
The remaining examples assume that label is `laptop`; append `--name laptop`
to the join command if you want that explicit label instead.

Sign in to the same tailnet in the browser when prompted. You do not need an API
access token on laptop or an enrollment key copied from desktop.

The coordinator address and default port (`50052`) are saved. You do not repeat
them on later commands.

## 3. Check the mesh from either computer

```powershell
herdr-mesh nodes
herdr-mesh doctor
```

Confirm both computers are connected and ready to manage Herdr sessions.
A running background process alone is not evidence that its worker is ready.

For the browser dashboard:

```powershell
herdr-mesh dashboard
```

For local connection status:

```powershell
herdr-mesh status
```

These commands reuse the existing connection. You can run another command while
watching output or using the dashboard.

The dashboard shows configured workspace/tab/pane/agent names, with stable IDs
secondary or as fallbacks. It shows workspace checkout paths and reported
pane/agent working directories; directories belong to the execution computer.
For ordinary workspaces without checkout metadata, a **Reported directories**
list shows distinct directories from contained panes and agents; it does not
guess a workspace directory. Names and directories are searchable and refresh with snapshots.
Automatic terminal titles, prompts, terminal contents, tokens, and provider
session-file paths are not part of this inventory.

Install the same updated build on the coordinator, execution nodes, and dashboard.
Upgrade the coordinator before nodes: older coordinators reject unknown inventory
fields. Older nodes shown by a new dashboard keep their ID-only fallback until
upgraded. Restart the mesh processes to use the new build; restarting only the
browser cannot add fields an older daemon does not send.

### Connected, but Herdr is not live

Existing Herdr session names may contain uppercase letters (for example `QEI`);
preserve their exact spelling. This is separate from the lowercase mesh node
label supplied to `init` or `join`. Keep coordinator and client mesh binaries
updated together so discovery and the dashboard both accept those session names.

**Connected** confirms the mesh connection, not successful Herdr discovery.
The node card distinguishes waiting for discovery, a discovery failure, no
reported sessions, and an unsupported session. It shows the discovery error
separately rather than treating a failed discovery as **Herdr disabled**.

For the read-only native/runtime commands and logs to collect on the affected
computer, see [Native discovery diagnostics](../README.md#native-discovery-diagnostics).
These are support diagnostics, not an additional mesh entrypoint or setup step.

The daemon logs discovery failures and session-state changes, including the
native protocol number, without repeating unchanged failures on every poll.
`invalid_discovery_response` means the native discovery JSON was not accepted;
`discovery_unavailable` means discovery could not run successfully.
`unsupported_protocol` is a native Herdr compatibility problem: mesh protocol
`1` in registration messages is a different protocol. Native `compatible: true`
only confirms that Herdr's own CLI understands its server.

Do not destroy mesh or Tailscale state to troubleshoot these errors. Reconnect
messages alone do not explain a native discovery failure. Before sharing logs,
remove browser sign-in URLs, credentials, and any private paths you do not want
to disclose; do not send managed databases or tsnet state.

### Dashboard cannot connect to the managed daemon

A missing private IPC endpoint (a named pipe on Windows) after a previous
**Ready** means the runtime is no longer listening; readiness is not a promise
that the process cannot fail later.
Check `herdr-mesh status` and the daemon log before attempting another enrollment.
If the coordinator is down, other computers may finish browser enrollment but
cannot complete `join`. For an installation with saved configuration, use
`herdr-mesh start` once the coordinator is healthy; do not destroy/recreate the
nodes. Repeat `join` only if initial setup did not save a configuration.

Persistence failures include an operation and attempt in the daemon log. The
coordinator retries a timed-out local database operation once only when the
store proves that no commit was attempted and any rollback completed. Unknown
commit outcomes, disk errors and repeated timeouts still stop the coordinator
rather than publish uncommitted state. This does not retry agent actions.

## 4. Register an existing project

Prepare an existing Git checkout on each computer where you want to run work.
Register its path centrally, from either computer:

```powershell
herdr-mesh project add demo --node desktop --path C:\src\demo
herdr-mesh project add demo --node laptop --path C:\src\demo
```

Each path belongs to the selected computer, not necessarily the computer where
you typed the command. The paths can be different. Registration does not clone
repositories or copy project files.

## 5. Run and observe a task

```powershell
herdr-mesh agent start smoke --node laptop --project demo --prompt "Say hello"
herdr-mesh agent follow smoke --node laptop
```

Start ensures a named `main` Herdr session, opens or reuses the project's existing
checkout workspace, and launches a dedicated Copilot agent named `smoke`.
It does not create a Git worktree or attach a Herdr frontend. Use `--session`
or `--provider` when you want a different session or provider.

Canceling follow stops watching, not the agent. To stop that agent:

```powershell
herdr-mesh agent stop smoke --node laptop
```

The application retains the underlying session and terminal identities for safe
targeting. You do not copy those IDs or invent retry keys. If a name is ambiguous
or its target has been replaced, the command stops with an explanation rather
than guessing. If a launch outcome is uncertain, inspect it instead of starting
another agent under a new name.

An agent name stays bound to its original launch, including after stop. Repeating
the same start is a retry, not a new launch. Once the previous outcome is known,
choose a fresh name for a genuinely new run.

Here `smoke` is the mesh control name chosen by `agent start`; `laptop` is the
mesh node label saved by `init` or `join`, derived from the OS hostname unless
overridden with `--name`. Neither is a workspace, tab, or Herdr session name.
Renaming a TUI display label does not rename the original mesh control name. Agents started
directly in the TUI do not automatically receive one of these mesh control names.

An accepted prompt is not proof the provider completed the task. Check the
actual response. Provider sign-in and permission prompts are not approved
automatically.

## 6. Shut down or destroy this installation

Stop only this computer's managed daemon:

```powershell
herdr-mesh shutdown
```

This **does not delete state**. It keeps enrollment, databases, configuration,
journals, tsnet state, and logs. Herdr sessions, provider agents, repositories
and worktrees stay untouched. Stopping the coordinator disconnects mesh control
for the other nodes; it does not stop their agents.

Resume the selected installation using its saved configuration:

```powershell
herdr-mesh start
```

There are no repeated join, name, or server flags. `start` launches the current
executable, not a saved path to an old binary. For an upgrade, shut down, replace
the executable in place, and run `start`. Copying only the executable elsewhere
selects fresh default state; `start` there requires an initialized installation.
To relocate an existing installation on the same computer, move the whole
installation directory while stopped or select the existing state directory
with the global override. Do not run two processes against the same state.

For a clean start, preview the destructive scope, then apply:

```powershell
herdr-mesh shutdown --destroy --remove-policy --dry-run
herdr-mesh shutdown --destroy --remove-policy
```

`--destroy` removes this computer's exact Tailscale device, then deletes its
managed configuration, databases, journals and tsnet state. It does not delete
the executable or anything outside the managed state
directory. You must type the displayed **mesh node label** to confirm.
`--yes` is an explicit confirmation bypass for controlled automation.

`--remove-policy` is optional and coordinator-only. It removes only additions
provably owned by this installation's original policy apply, with concurrency
protection. Existing rules and unrelated changes are preserved. If ownership
cannot be proved, or other devices depend on those additions, cleanup stops
rather than removing shared access. Without this flag, tailnet policy is retained.

Destruction needs a **Tailscale API access token**, prompted without echo; a
device enrollment key is not sufficient. No token is required for ordinary
shutdown. For automation, `--api-token-env` names a privately supplied environment
variable, never an inline credential. Authorization and policy conflicts are
checked before teardown. A dry run performs no teardown and does not certify
remote authorization.

Local recovery data is retained when remote cleanup is incomplete or uncertain.
Resume with the same flags; do not delete the retained destroy record or change
identity to bypass an unknown result. A destroy intent prevents `init`/`join`/`start`
from restarting the retiring installation. Once completed, run `init`/`join`
normally to create a fresh identity. Destroying journals intentionally discards
retry history; do not reuse old command receipts or retry old work afterward.

New daemons shut down cooperatively on all supported platforms. The Windows
legacy fallback uses a verified same-user, exact-command-line process stop,
never a process-name or process-tree kill; unsupported or unverified fallback
stops fail closed. Inspect any interrupted work before retrying it.
Deleting only the local state folder does not unregister remote Tailscale
devices or remove policy; use the guarded destroy flow for remote cleanup.

## Existing installations and advanced operations

There is no silent AppData migration and no automatic cleanup of old startup
registration. For a clean start, shut down and destroy an old installation with
its old version before installing this version. The new version does not read,
migrate, or delete the old registration. A clean installation enrolls fresh
state beside its executable.

Current managed configuration is checked rather than silently overwritten.
Do not delete identity directories to bypass an incomplete remote cleanup.
Advanced role defaults are separate directories under
`<executable-directory>\herdr-mesh-state\advanced\<role>`; they are not additional
setup steps for the shared managed installation.

`agent stop` stops an agent, not the daemon. Use `shutdown` for the daemon and
explicitly opt into `shutdown --destroy` for managed deinitialization.
The advanced per-role maintenance commands are not a managed uninstall path.

For MCP, configure your MCP client to run:

```powershell
herdr-mesh mcp
```

For explicit role management, worktree creation, detailed receipts, and offline
maintenance, use the [advanced operator reference](advanced-operator-guide.md).
Its per-role backup commands do not back up the shared managed installation;
do not apply them to individual folders inside the managed state directory.
The [Windows acceptance matrix](windows-live-validation.md) is for deeper
release validation, not a prerequisite for trying the mesh.
