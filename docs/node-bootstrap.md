# Bootstrapping a Windows mesh node over an existing connection

**`herdr-mesh bootstrap`** is the single operator entrypoint for Windows-first
**staged install and optional existing-runner startup**. The Go executable
embeds its PowerShell implementation: no source checkout, dot-sourcing,
separately shipped script, or caller-owned PSSession is needed.

The command selects an explicit, already-prepared **SSH host alias** from the
operator's existing user `.ssh\config`. It creates one bounded, noninteractive
SSH-based PowerShell remoting connection to the existing `powershell`
subsystem, using the operator's previously established authentication and
host trust. A Go process cannot import another PowerShell process's live
PSSession; WinRM/session import is not this CLI's transport. Bootstrap does not
establish trust, obtain credentials, configure SSH/endpoints, or enroll a node.

The default outcome is **`staged-not-started`**. Opt-in
`-start -existing-task '\HerdrMesh\Node'` can start or reuse exactly one
**already-configured Windows Scheduled Task**, after validating its direct
node action and persistent-runner settings. A successful start observation is
**`runner-running-mesh-unverified`**, never mesh or provider readiness.
Nothing creates, modifies, stops, or repairs tasks, services, accounts,
credentials, global tools, or providers. `Start-Process` from an SSH/WinRM
session is not used as a substitute for a persistent runner.
No live remote rollout is implied by the local fixture tests.

## Prerequisites

- Windows and existing **PowerShell 7.4+ (`pwsh`) and OpenSSH (`ssh`)** on the
  controller's PATH. The target must already have Windows, PowerShell 7.4+ and
  a working SSH `powershell` remoting subsystem supporting the fixed script
  and .NET file operations. Nothing installs tools globally or changes an
  execution policy globally, an endpoint, a service, or an SSH setting.
- An existing nonempty user `.ssh\config`, at most 64 KiB, containing an explicit
  literal `Host` entry for `-ssh-host`. Multi-name `Host` entries are allowed;
  wildcard-only aliases, `Include`, `Match`, `SendEnv` and `SetEnv` directives
  are not supported. Use a direct, operator-reviewed alias/configuration and
  existing known-hosts entry. Custom config-file parameters, proxy/jump
  commands, SSH command strings and extra SSH arguments are not exposed.
  The normal OpenSSH system configuration is also part of the operator's
  existing trusted tool setup; do not modify configuration during bootstrap.
- Public-key authentication must already work without prompts (for example,
  through the operator's existing local agent or prepared key configuration).
  An encrypted key requiring an interactive unlock must be prepared separately.
  No password, username override, private-key, enrollment-key or credential
  parameter is accepted. Unknown/changed host keys fail instead of prompting.
- A trusted **local Windows release ZIP** and a mandatory expected SHA256
  obtained through a trusted channel. See `release-and-recovery.md` and
  `scripts\build-release.ps1`. The Windows amd64/arm64 archives produced there
  contain exactly one root entry, `herdr-mesh.exe`, and are compatible. Linux
  and macOS artifacts are not supported by this bootstrap. Unsigned checksums
  do not authenticate a publisher; calculating a hash from an untrusted download
  does not make it trusted. Copy the ZIP to a fixed local drive first; network,
  removable and UNC/device archive paths are not accepted.
- On the target, an **existing private install parent** and an **existing,
  separate private persistent state directory**, on fixed local disks. The
  install directory itself must not exist for publication. With `-start`, an
  already-staged matching install may be verified and reused without any
  upload or overwrite. Paths must not overlap. Bootstrap's file operations
  never create, copy, read for identity material, change, back up, or delete
  state files or state-directory ACLs. A started node naturally uses its own
  persistent state.
- The connected account must be able to read/write the install parent. Its
  owner and allowed ACL identities must be limited to that account, SYSTEM,
  and local Administrators (inheritable CREATOR OWNER is also permitted).
  The same conservative ACL preflight applies to the state directory.
  Reparse points anywhere in target path ancestry are refused. The operator
  remains responsible for the privacy of preexisting state contents and for
  preventing privileged concurrent path/ACL changes; this is not a hostile
  multi-user filesystem sandbox.
- Enough space for at most **256 MiB of ZIP plus 512 MiB of executable** and a
  small receipt, per attempt. Failed attempts are retained for inspection, so
  repeated attempts also consume space. There is no background cleanup.
- For `-start`, the target must already provide the Windows `ScheduledTasks`
  commands `Get-ScheduledTask` and `Start-ScheduledTask`. The existing task,
  account permissions, noninteractive logon, enrollment and provider
  environment must already be prepared; see the exact contract below.
  Missing prerequisites fail rather than triggering installation or repair.
- For headless mesh configuration, supply optional `-herdr-executable` pointing
  to an **already-existing target-side local `.exe` file**. Herdr installation
  and the intended runner account's access remain operator prerequisites.
  Bootstrap checks the path and existence, not the executable's identity,
  version, protocol compatibility, or headless/provider readiness. It never
  copies, installs, or directly executes Herdr.

Directory arguments are explicit target-side paths, not local environment
expansions: absolute drive-letter paths, at most 200 ASCII characters, with
ordinary letters, digits, spaces, dots, underscores and hyphens in components.
Components must start with a letter, digit or underscore and be at most 80
characters. Roots, trailing separators/dots/spaces, dot segments, device names,
UNC/device paths, alternate data streams, and shell metacharacters are refused.
The optional Herdr executable uses these same path/component bounds and
reparse-point restrictions, must end in `.exe`, and must be a file on a fixed
local disk rather than a directory or wrapper script. It is a **target-side
path**, not a local-controller executable or a command plus arguments.
The coordinator is a DNS name or IPv4 address with port `1..65535`; URLs,
IPv6 literals, and extra argument text are not accepted in this initial surface.

## Single-command operator invocation

Run the released executable from an operator shell. The alias, host-key trust,
authentication, target directories, Herdr installation and (with `-start`)
Scheduled Task are already prepared. No separate script or session setup step
is performed by the operator as part of this invocation.

```powershell
herdr-mesh bootstrap `
    -ssh-host mesh-node-a `
    -archive '.\herdr-mesh-1.2.3-windows-amd64.zip' `
    -sha256 '<64 hex characters from the trusted release checksum>' `
    -architecture amd64 `
    -install-dir 'C:\private\mesh-releases\1.2.3' `
    -state-dir 'C:\private\mesh-node' `
    -server herdr-mesh-server:50052 `
    -herdr-executable 'C:\Tools\Herdr\herdr.exe' `
    -start -existing-task '\HerdrMesh\Node' -json
```

`-ssh-host`, `-archive`, `-sha256`, `-architecture`, `-install-dir`, `-state-dir`
and `-server` are required. `-timeout` defaults to `5m` and accepts whole-second
durations from `1s` through `30m`, including local validation and connection.
Substitute your prepared target paths and the coordinator's actual assigned
MagicDNS name. The Herdr path must be an absolute existing target-side `.exe`,
not the bare `herdr` shorthand accepted by a locally started node.
Use `herdr-mesh bootstrap -help` for the complete flag list.
Supplying `-herdr-executable`
appends exactly `-herdr-executable`, followed by that path, to the fixed node
argument array. This **configures** named-session and central project-management
capabilities without requiring a default socket; it does not verify them.
Omitting it retains the original transport-only node configuration.
`-start` and `-existing-task` must be supplied together; omitting both means stage
only and does not query the scheduler. The full task name includes its leading
backslash and any folders; no wildcard or task enumeration is supported.
There is no force, overwrite, upgrade-in-place, resume, command string,
extra-arguments, credential, enrollment-key, or remote download parameter.

The SSH alias is bounded to 64 ordinary ASCII letters/digits/dots/underscores/
hyphens, not `user@host`, a URL, or an SSH option string. The embedded entrypoint
sets `BatchMode=yes`, `StrictHostKeyChecking=yes`, public-key-only authentication,
zero password prompts, and one connection attempt. It disables host-key updates,
DNS host-key trust, agent/X11 forwarding, port forwarding, local/proxy/remote
commands, multiplexing, and askpass. A connection handshake has at most 30
seconds and also respects the overall deadline. These are invocation options,
not edits to the operator's SSH configuration or known-hosts files.

Typed request values are serialized into a private, bounded local JSON file,
never interpolated into PowerShell source or passed in the child process argv.
Embedded assets are materialized in an ACL-protected temporary directory,
removed after execution, and are not shipped separately. The local script
process strips unrelated credential/provider/enrollment environment variables
before starting SSH. The local agent channel can remain available for existing
SSH authentication, but agent forwarding is disabled.
Output is bounded and schema-checked; raw SSH/task/process diagnostics and
unexpected output fields are not forwarded to the CLI.

## Install/verification contract

Before any OOB request, the controller locks the local archive against writes
for the duration of validation/upload, verifies its SHA256, checks its sole
entry and size bounds, and hashes the decompressed binary. It checks the
Windows PE32+ header and architecture without executing the binary. This is
static format/integrity verification, **not** a Windows loader test, signature
verification, `version` execution, Herdr readiness probe, or mesh health check.
The selected architecture must also match the target's native OS architecture.
ZIP metadata must use a single disk, one entry, and a central directory no
larger than 64 KiB; ZIP64 and nonstandard trailing data are not accepted. These
bounds are checked before the ZIP library materializes its entry collection.

The only OOB actions are fixed `prepare`, `upload`, `commit`, and optional
`start` operations.
Arguments are serialized data passed to a constant PowerShell scriptblock;
paths, addresses, and archive bytes never become source code or an unrestricted
shell command. Uploads are sequential, at most **192 KiB per chunk** (256 KiB
base64 plus small metadata), with exact offsets and total-size enforcement.
The endpoint must permit this bounded remoting payload; restrictive endpoint
quotas fail explicitly and are not increased by bootstrap.

The target stages under
`<install-parent>\.herdr-bootstrap-<operation-id>`, recomputes the archive and
binary hashes, and extracts only the exact `herdr-mesh.exe` entry to a new
file. ZIP paths are never used as extraction destinations. Extra/duplicate
entries, directory/link entries, traversal, wrong architectures, mismatched
hashes and oversize content are refused. An atomic same-parent directory rename
publishes the three files without overwriting any existing destination:

| File | Purpose |
| --- | --- |
| `herdr-mesh.exe` | Statically verified; only the optional validated task can be requested to execute it |
| `release.zip` | Original verified archive, retained for provenance |
| `bootstrap-receipt.json` | Non-secret, versioned staged-install receipt |

The persisted installation receipt and CLI result include archive/binary SHA256,
operation ID, architecture, explicit install/state paths, `Executable`, and
the fixed argument **array**
`["node", "-server", "<coordinator>", "-state-dir", "<persistent-state>"]`.
When `-herdr-executable` is supplied, it becomes exactly
`["node", "-server", "<coordinator>", "-state-dir", "<persistent-state>", "-herdr-executable", "<target-herdr.exe>"]`.
The optional pair is part of the persisted receipt, reuse verification, and
Scheduled Task's exact-match contract. No separate free-form argument field
is introduced. The original five-argument schema-1 transport-only receipts
remain reusable when this option is absent.
It is data for an operator's runner configuration, not an escaped shell command.
The installation receipt records publication, not live runner state:

```json
{
  "Status": "staged-not-started",
  "Startup": "not-attempted",
  "Enrollment": "not-checked",
  "Connectivity": "not-checked",
  "RunnerRequired": true
}
```

The immutable on-target installation receipt retains its schema-1 PascalCase
field names for compatibility. CLI `-json` results use snake_case, including
`status`, `operation_id`, `node_arguments`, `startup`, `enrollment` and
`connectivity`. Without `-json`, the CLI prints a compact lifecycle summary.

No enrollment keys are accepted, transported, logged, or written to receipts.
Raw remote errors and unexpected remote properties are not copied into output.

## Existing Scheduled Task contract

The explicitly named task must already be registered on the target. Its
executable may point to the new, not-yet-published install path. Bootstrap
validates the task **before creating staging artifacts**, revalidates before
extraction/publication, and validates again immediately before deciding
whether to request a start. Task definitions and raw argument strings stay
inside the remote process; errors expose categories, not their contents.
When supplied, the Herdr executable path is checked before staging and
rechecked before publication and task dispatch, including reuse of an existing
install. Missing or reparse-point targets fail without being installed or
repaired. A disappearance after staging can leave staged artifacts; it does
not authorize publishing or starting against a missing prerequisite.

| Setting | Required value |
| --- | --- |
| Task name | Full name such as `\HerdrMesh\Node`; at most 200 ASCII characters, components at most 80, starting with a letter/digit/underscore; no wildcards, expressions, trailing dots/spaces |
| Actions | Exactly one `MSFT_TaskExecAction`, directly executing the exact absolute installed `herdr-mesh.exe` path |
| Arguments | Exact canonical string shown below, including order, quotes and spacing; no wrappers, additional flags or actions |
| Working directory | Empty, or the exact install directory |
| Multiple instances | `IgnoreNew`; `Parallel`, `Queue`, and `StopExisting` are refused |
| Enabled / demand start | Both enabled |
| Execution time limit | `PT0S`, meaning no scheduler execution-time limit |
| Principal logon type | Already-configured `Password`, `S4U`, or `ServiceAccount`; interactive/session-dependent logon modes are refused |
| Current state | `Ready`, `Running`, or `Queued`; disabled/unknown states are rejected |

For the example above, the task action must have these separate fields:

```text
Execute: C:\private\mesh-releases\1.2.3\herdr-mesh.exe
Arguments: node -server "herdr-mesh-server:50052" -state-dir "C:\private\mesh-node" -herdr-executable "C:\Tools\Herdr\herdr.exe"
```

The executable field is not a quoted command line. The argument string always
quotes the server, state, and optional Herdr executable values. Omit precisely
the final `-herdr-executable "<path>"` pair for a transport-only task when the
option is absent. Semantically similar alternate quoting,
flag ordering or added whitespace is intentionally not accepted. No task
arguments supplied by the operator are ever evaluated as PowerShell.
Executable and working-directory path comparison is case-insensitive; use
the exact canonical argument values and task-name spelling.

If the task reports `Ready`, bootstrap issues **at most one**
`Start-ScheduledTask -InputObject <validated-task>` call. If it already reports
`Running`, bootstrap reuses it without another start; if it reports `Queued`,
bootstrap observes that pending run without enqueueing another. `IgnoreNew`
also guards the ready-to-start race with another trigger for this same task.
It does not protect against another task or unrelated process sharing state:
the operator must ensure exclusive state ownership.

Bootstrap then obtains fresh task definitions/states, rechecking the action
and settings on each observation, for **at most ten seconds**, further bounded
by the operation's remaining deadline. It does not reuse the pre-start state
as evidence of startup or retry if a task exits. Scheduler conditions can
delay or prevent startup; task state is a momentary observation, not a promise
of future uptime, proof of the loaded process image, or a mesh health check.
Do not concurrently edit the task, binaries, directories or ACLs during this
workflow. Task validation and scheduler dispatch are separate OS operations,
not an atomic transaction against a hostile task administrator.

On a fresh `Running` observation, CLI `-json` includes:

```json
{
  "status": "runner-running-mesh-unverified",
  "startup": "requested-once",
  "existing_task_name": "\\HerdrMesh\\Node",
  "runner_state": "Running",
  "enrollment": "not-checked",
  "connectivity": "not-checked",
  "runner_required": false
}
```

`startup` can instead be `reused-running` or `observed-pending`. The result
also includes `runner_observed_at_utc` from the target, the current bootstrap
`operation_id`, and the original `install_operation_id`. The immutable
`bootstrap-receipt.json` remains an installation record; it is never rewritten
to claim a task is still running.

To start a previously staged install, invoke with the **same trusted archive,
hash, architecture, install/state paths, server, and optional Herdr executable
(including whether it was supplied)**, plus `-start` and the
matching task name. The existing receipt, archive and executable are verified
again; a mismatch fails without replacement. This is read-only reuse, not an
updater or automatic recovery/retry mechanism.
Adding, removing or changing the Herdr option does not silently reconfigure
an existing installation receipt or task: reuse rejects the mismatch. Use a
new absent install directory and an operator-prepared matching task for a
changed configuration.

## Enrollment and recovery boundary

For a first node enrollment, the operator must separately supply the
node-specific `TS_AUTHKEY_NODE` through the runner's protected environment
and the expected `tag:herdr-mesh-node` tailnet role. Never distribute server or
controller credentials to a node or copy another node's enrolled state.
Existing state can be reused by its intended node. The task's account must
already have the intended state access and environment; environment variables
set in a remoting session are **not** automatically inherited by a Scheduled
Task. No credentials are read or changed by this command.

Verify actual connectivity and role checks through normal **`ctl nodes`**
inventory from an appropriately enrolled controller after startup. Never use
the running node's state directory for the controller. A running task alone
does not establish enrollment, node registration, a fresh heartbeat, Herdr
readiness, or provider success. The OOB connection is
**bootstrap transport only**; it grants no mesh role, replaces no tsnet `WhoIs`
trust, and is not retained as a mesh control channel.

Without `-herdr-executable`, the fixed task arguments remain transport-only.
With it, the node is configured for named headless sessions and central project
management without a default socket. **Configured is not verified**: neither
file existence nor a `Running` task establishes those capabilities' readiness.
No other Herdr/provider arguments or a general shell command are accepted.
Herdr installation, other runner configuration such as `-herdr-socket`,
Git, provider
installation/authentication/approval and headless compatibility remain
separate preparation steps. Bootstrap does not depend on project
authorization, grants, a project catalog, or provider control.

An existing install is refused unless `-start` verifies and reuses it without
overwriting. For a later version, stage to a new
absent versioned directory with the **same disjoint state path**. This does not
stop or switch an existing runner. An opt-in task must already target that new
version. Follow `release-and-recovery.md` to stop
only the intended old process, back up stopped state, and switch/restart the
operator's runner. Never run two processes sharing state.

Failures return a nonzero CLI error with an actionable sanitized category.
With `-json`, operation failures also emit a bounded object with `code`,
`status`, and, when available, `phase` and `operation_id`. Invalid flags never
echo supplied values. Before dispatch, the outcome is `not-installed`; after staging
OOB dispatch it is conservatively **`indeterminate`**, even if failure probably
happened before a write. Initial absent/mismatched runner prerequisites fail
before staging mutations; a subsequent configuration change can leave partial
staging but cannot authorize publication/start without revalidation.
No automatic retry or rollback is performed. A lost
commit acknowledgement can mean the complete install was published despite
the caller receiving an error. Inspect the intended install path, its receipt
and hashes, and the exact staging path from the operation ID before deciding
what to do; do not blindly retry, overwrite, append chunks, or remove state.
Only manually remove positively identified abandoned staging artifacts when
no remoting work can still be using them.

Once the OOB `start` operation might have been dispatched, a start failure,
lost acknowledgement, malformed reply, expired observation window,
cancellation or deadline returns a terminating error with
**`status = start-unknown`**. It is never a successful install/start result;
mesh, enrollment and provider readiness remain unverified.
Even an error from the scheduler's start call may follow dispatch. Inspect
the named task and normal mesh inventory before deciding to invoke again;
there is no automatic start retry, task stop, rollback, or task repair.
An explicit later invocation may reuse a running matching task, but is not
a safe blind retry if a prior start's outcome is unresolved.

The overall Go context/deadline includes local verification, connection and
remote work. The private script host owns the local PowerShell/SSH process
tree and terminates it on cancellation, timeout or output overflow.
Active remoting jobs
are polled for deadline/cancellation and stopped/removed on exit; remote loops
also have a bounded remaining-time budget. Remoting job submission and
best-effort job stopping are subject to the existing connection's transport
timeouts. Neither a timeout nor Ctrl+C guarantees that the remote operation
stopped, that a final rename did not happen, or that a requested task did not
start. Stopping/removing a remoting job never stops the Scheduled Task.
If the local process ends before a valid envelope is received, the CLI
conservatively reports `indeterminate`, or `start-unknown` when `-start` was
requested, even if remote dispatch probably had not happened yet.
Such a hard local failure may not yield an `operation_id`; inspect the
intended install/task and parent directory rather than guessing a staging ID.
Inspect before retrying.

## Developer-only validation and parent integration

```text
go test .\src\internal\bootstrap .\src\internal\app -run 'Test(Bootstrap|OptionsBoundaries|Embedded|PrerequisitesNoFallback|CancellationAndUnknown|ProcessFailures|ResponseBounds|PrivateFakeProcess|FakeProcess)'
pwsh -NoLogo -NoProfile -NonInteractive -File .\scripts\test-bootstrap-node.ps1
```

These are source-checkout developer checks, not operator entrypoints or release
dependencies. `scripts\bootstrap-node.ps1` is only a thin developer fixture
wrapper around the embedded asset. The PowerShell suite uses a uniquely named
disposable local Windows directory and fake SSH/PSSession/OOB job and
ScheduledTasks cmdlets. The actual fixed remote script and production orchestration
perform archive checking, chunk staging, ACL/path checks, state preservation,
and publication against those fixtures. No PSSession, SSH/WinRM connection,
node process, real task registration/query/start, provider, enrollment, or live
machine is created or contacted.
It covers injection boundaries, archive/hash/size rejection, no-overwrite and
side-by-side state preservation, upload/publication failures, lost replies,
missing/mismatched runners, wrappers/multiple actions, duplicate policies,
existing running/queued task reuse, task definition changes, one-shot startup,
start failures/lost acknowledgements, cancellation, deadlines, the ten-second
observation bound, and truthful lifecycle receipts.
It also covers explicit Herdr paths, missing/invalid/reparse targets, exact
task/receipt argument matching, identical headless reuse, disappearance before
publication/start, and backward-compatible transport-only staging/reuse.
Go tests cover typed parsing, bounded fake host requests/responses, cancellation,
error redaction and embedded assets. Disposable local PowerShell fake-process
fixtures additionally verify ACL-protected request/assets, values absent from
argv, cleanup, output caps and process deadlines; these fixture scripts contain
no SSH, scheduler or node operations. Fake SSH-entry tests verify the actual
strict option generation, alias/config preflight, environment filtering,
structured argument forwarding, no retries and truthful cleanup failures.
Real remote connectivity, endpoint serialization/quotas and persistent runner
behavior are not established by these tests.

**Parent router hook** (the bootstrap worker does not own `app.go`):

```go
case "bootstrap":
    return RunBootstrap(ctx, args[1:], streams)
```

The exact exported signature is
`func RunBootstrap(ctx context.Context, args []string, streams IO) error`,
where `args` excludes `bootstrap`. Add `bootstrap` to parent-owned root help.
The handler uses `internal/bootstrap` and the existing private `scripthost`;
no identity, tsnet, protocol or project-catalog dependency is introduced.
`go:embed` includes the implementation and entrypoint in the normal release
binary; no release-script change or additional shipped file is needed.
The CLI operator examples require that parent-owned router hook.

Scheduler API references:
[Get-ScheduledTask](https://learn.microsoft.com/en-us/powershell/module/scheduledtasks/get-scheduledtask)
reads the local task definition, while
[Start-ScheduledTask](https://learn.microsoft.com/en-us/powershell/module/scheduledtasks/start-scheduledtask)
requests an asynchronous registered-task start. These calls are made **inside**
the existing remote process, without an additional CIM connection.
