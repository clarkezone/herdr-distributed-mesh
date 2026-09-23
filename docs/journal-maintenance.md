# Offline journal maintenance

This narrow maintenance surface inspects durable state and creates/verifies
complete offline **mesh role** backups. It is not an executor, command-history
browser, SQL interface, pruning tool, or uncertainty-resolution service.
It follows the restore cautions in [Release, installation, and recovery](release-and-recovery.md).

These commands are for advanced, independently configured roles, whose defaults
are `<executable-directory>\herdr-mesh-state\advanced\<role>` on all platforms,
not AppData or the current working directory. They do not back up the shared
managed installation, and must not be applied to individual subdirectories of it.

Managed `init`/`join`/`start` run a background process on Windows, Linux, and
macOS without registry access, sign-in autostart, boot services, or scheduled
tasks. Its configuration, databases, journals, tsnet identity, and logs live in
`herdr-mesh-state` beside the executable. `herdr-mesh shutdown` preserves this
state; `herdr-mesh start` reads its saved configuration and launches the current
executable without repeated join, name, or server flags. An optional absolute
global override precedes the command, for example
`herdr-mesh --state-dir C:\private\mesh-managed start`; use the same selection
for shutdown and other managed commands.

Shut down before replacing the executable in place. Moving the whole
installation directory on the same computer, or explicitly selecting its
existing state, allows restart independent of the old executable location;
copying only the binary elsewhere selects fresh default state. Never store or
sync tsnet identity to multiple computers, or use OneDrive as live state
storage. A synced binary is a delivery artifact, not a live installation.

There is no silent AppData migration or old startup-registration cleanup.
For a clean start, shut down and destroy the old installation with its old
version. The new version does not read, migrate, or delete old registration.
Current managed destruction uses `shutdown --destroy`, with guarded remote
cleanup and Tailscale API authorization; deleting a local folder alone does not
unregister remote devices or remove policy. Offline maintenance is not an
alternative uninstall or remote cleanup path.

## Runtime ownership

The `maintenance` command is wired into the production CLI. Server, node, ctl,
doctor, standalone dashboard, and MCP startup now participate in full-role
locking. The lock covers the role root containing `instance-id`, its journal,
and the nested `tsnet` directory, not just the tsnet subdirectory.

`state.PrepareRoleState` acquires the lifetime guard before identity, journal,
or tsnet access. Server and node use distinct role markers; all controller
surfaces use `client` so sequential reuse remains valid. Injected controller
clients reuse the outer connection's guard rather than opening a second one.
Activation happens only after the runtime owns its journal and tsnet identity.
The guard remains held until network shutdown and all state writers finish;
acquisition, activation, and close failures are propagated.

`AcquireRoleState` requires an existing absolute local directory with no linked
ancestors. It creates a private, kernel-locked `role-state.lock` containing a
versioned **pending** role marker. Acquisition alone does not confirm that an
older transport-only process has relinquished its tsnet identity. If startup
aborts before activation, closing the guard leaves the marker pending and backup
still fails closed. A later legitimate startup can reacquire that marker and
activate it after obtaining the actual writer locks. Interrupted pending-marker
creation or activation is recoverable at startup without granting maintenance
permission. `Activate` is idempotent and rejects a closed guard.

Valid established v1 active markers remain compatible and are not rewritten;
new active markers retain their pending header plus a flushed activation record.
Maintenance never creates, repairs, or activates markers. It must not be used
to manufacture evidence of a successful runtime startup. Keep older/unmodified
binaries and other unaware writers away from a directory after adopting the
new contract: a marker cannot make an old program honor an unfamiliar lock.
Unsupported downgrade/reuse with an unaware writer is not made safe by either
an established marker or this activation API.

Why the startup hook is necessary: an available SQLite ownership lock proves
that no cooperating journal owner is running. It does not exclude a
transport-only node or another role writing the same tsnet identity directory.
Full-role backup therefore fails explicitly without the runtime lock contract.
Inspection of an existing journal works without this newer marker, but still
requires its existing journal lock to be free.

## Commands

Stop only the exact role process using its state directory. Never remove a lock
file to bypass ownership, and never copy enrolled identity to a second running
machine.

```powershell
herdr-mesh maintenance inspect -role node `
  -state-dir 'C:\private\mesh-node' -json

herdr-mesh maintenance backup -role node `
  -state-dir 'C:\private\mesh-node' `
  -destination 'D:\private-backups\mesh-node-before-upgrade' -json

herdr-mesh maintenance verify-backup -role node `
  -state-dir 'D:\private-backups\mesh-node-before-upgrade' -json
```

Use `-role server` for coordinator state. Every operation requires an explicit
absolute `-state-dir`; there is no default that could select the wrong role.
Replace the illustrative source path with that stopped role's actual state
directory; an advanced node launched with defaults uses
`<executable-directory>\herdr-mesh-state\advanced\node`, not
`C:\private\mesh-node`.
For `verify-backup`, that flag names the backup container, not its nested
`state` directory. `-destination` is accepted only for `backup` and must not
already exist. Parent directories must already exist.

The default timeout is 30 seconds, with a maximum of two minutes. Filesystem
syscalls on a failing local disk cannot always be interrupted, but SQL and
chunked copies obey cancellation. Network/UNC locations, linked/reparse
ancestors, hardlinked state files, nonregular files, path traversal, and
backup destinations inside the source tree are rejected. State uses the
standard layout: `coordinator\mesh.db` for a server, `commands\journal.db` for a
node, `instance-id`, and `tsnet\tailscaled.state`. Custom external state stores
and journal-only folders are not whole-role backups.

### Inspection and capacity

Inspection takes the existing journal lock (and the role lock when present),
runs SQLite integrity/schema/owner checks, and validates retained typed journal,
project, and observation records. SQLite is opened read-only; inspection does
not run migrations, `Recover`, checkpoints, row updates, or command execution.
SQLite may use read coordination in existing WAL/SHM sidecars; retained database
records are not changed. A hot rollback journal requiring SQLite repair fails
closed rather than being recovered implicitly.

Output includes only role, schema version, integrity, journal slots
used/limit/remaining/full, and counts of accepted/running/indeterminate commands
and pending node receipts. No receipt listing, IDs, fingerprints, prompt bodies,
peer identities, enrollment material, checkout mappings, or local source paths
are emitted. A locked, corrupt, incompatible, canceled, or missing store produces
an explicit error rather than a success-shaped empty report.

At capacity, the supported action is **diagnostics and preservation**, not
deletion. Stop admitting new mutations and preserve both sides' journals.
Known retry identities and receipts must remain available. Do not change
request keys/project IDs, clear delivered results, discard tombstones, rotate
identities, reset journals, restore an older backup, or run VACUUM as a supposed
way to regain slots. Slot capacity is a retained-row limit, not free-page space.
A non-destructive future capacity increase must update the actual admission,
reader, migration, and validation bounds together and be tested against the
retained state before upgrading. This command cannot safely override those
compiled bounds; no pruning or capacity-bypass option is provided.

Running and uncertain claims are preserved exactly. Inspection never converts
them into success, and backup verification never declares their effects undone.

### Backup and recovery boundaries

Backup holds both ownership locks, first validates the source journal, and copies
the entire role directory, including hidden state, database sidecars, instance
identity, tsnet enrollment state, project caches, and unknown future state files.
It does not selectively export commands. External Herdr/provider state and Git
checkouts are outside the mesh role directory and need their own coordinated
backup strategy; this tool never traverses configured checkout paths.

The output is a newly reserved private directory:

- `state`: complete role-directory contents;
- `manifest.json`: private relative-path inventory, sizes, SHA-256 digests, and
  the source's aggregate inspection report;
- `COMPLETE`: flushed manifest digest, published only after copying and source
  rechecking finish.

Copied files/directories receive private permissions rather than retaining
possibly broader source ACLs. This is a file-content backup of mesh state, not a
filesystem image preserving arbitrary executable modes, alternate data streams,
or external mounts. Use trusted local filesystems and cooperating upgraded
processes: ownership locks and before/after checks are not an atomic snapshot
against a hostile local writer that deliberately ignores those locks.

File contents are flushed before completion. Unix directory entries are also
synced; Windows uses flushed files and a separate flushed completion marker,
not an unsupported directory `File.Sync`. Source content is rechecked while
locks remain held, and the destination is hash-verified before success is
reported. Operations are bounded to 4,096 entries, 512 MiB of source contents,
16 levels of descendants, and a 32 MiB manifest. Limits reject the whole
operation rather than silently omit files. Oversized logs/state need a separately
reviewed backup method; this tool does not delete them.

Failures leave the explicitly requested destination as an incomplete artifact;
there is no recursive cleanup or overwrite retry. An absent, malformed, or
mismatching completion marker fails verification. Verify again after copying
backup media. SHA-256 checks detect accidental alteration, not a malicious
party that can rewrite both manifest and completion marker. Keep the backup,
its private manifest, and credentials on trusted storage.

Verification is read-only and checks all role-tree contents against the complete
manifest. It does not open the copied SQLite database or write new sidecars.
Do not run a mesh process directly inside the backup artifact.

**No automatic restore is offered.** Verification proves artifact consistency,
not that its deduplication history is current. Before a manual recovery, stop
all affected writers, preserve the current complete directories, account for
every effect since the backup, and use a compatible binary. Never overwrite or
merge a live directory, mix a newer identity with an older journal, restore only
the `.db` file, or run original and restored enrolled state simultaneously.
Replaying a request after restoring a backup that predates its receipt can
duplicate the effect. If those facts cannot be established, retain the evidence
and leave mutations blocked rather than invent a safe result.

## Implemented interfaces and tests

Application router:

```go
func RunMaintenance(ctx context.Context, args []string, streams IO) error
```

State entry points:

```go
func AcquireRoleState(ctx context.Context, stateDir, role string) (*RoleStateLock, error)
func (guard *RoleStateLock) Activate() error
func InspectMaintenance(ctx context.Context, stateDir, role string) (*MaintenanceReport, error)
func BackupMaintenance(ctx context.Context, stateDir, role, destination string) (*MaintenanceBackupReport, error)
func VerifyMaintenanceBackup(ctx context.Context, backupDir, role string) (*MaintenanceBackupReport, error)
```

Focused disposable-directory coverage:

```text
go test ./src/internal/state ./src/internal/app -run '^TestMaintenance' -count=1
```

Tests cover both role formats, actual journal/role ownership locks, missing
runtime capability, aborted startup before activation, recoverable pending
markers, established-marker compatibility, corruption/identity mismatch, exact capacity, retained
running claims/tombstones, complete private-state copies, changed/incomplete
backups, cancellation, linked paths, destination nesting/overwrite, depth
bounds, parser restrictions, and redacted reports. They start no mesh, tsnet,
Herdr, or provider process.
