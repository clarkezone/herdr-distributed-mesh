# Release, installation, and recovery

The CI matrix runs ordinary tests, vet, builds, and dashboard model tests on
Windows, Linux, and macOS. Linux additionally runs race checks; Windows runs the
mocked tailnet-configuration and bootstrap boundary suites. This is separate
from opt-in real Herdr/provider and two-machine tailnet acceptance: a cross-build
or a green unit-test job does not establish provider compatibility on that OS.

## Build artifacts

On Windows with the repository's Go toolchain and PowerShell:

```powershell
.\scripts\build-release.ps1 -Version <release-version>
```

The script produces ZIP archives for Windows, Linux, and macOS, each for amd64
and arm64, plus `SHA256SUMS`, under `dist\<release-version>`. It embeds the version,
uses the existing production entry point/assets, and disables CGO. Use `-Targets`
to select a subset and `-OutputDirectory` for a different destination. Existing
target directories/archives/manifests are not overwritten. A failed build leaves
its explicitly named partial artifacts for inspection and produces no new
checksum manifest. Build from the intended clean source revision for release;
Go VCS metadata remains available with `go version -m`.

Each ZIP is checked to contain exactly one public executable, `herdr-mesh`
(`herdr-mesh.exe` on Windows). Production has one CLI entrypoint; role-specific,
milestone, helper, and experiment binaries are not release artifacts. Archived
transport experiments remain outside the production source entrypoints.

These are unsigned artifacts, not an installer or self-update mechanism.
Checksums detect corruption only when obtained through a trusted channel; they
do not authenticate the publisher. Nothing uploads or publishes a release.
Code signing and platform notarization require separately managed credentials.

An optional coordinator-hosted dashboard port requires its own all-sources-to-server
tailnet grant. `herdr-mesh setup tailnet -tailnet '<tailnet-name>'
-dashboard-port <port>` proposes that port-specific grant; omitting the option
preserves the existing RPC-only defaults.
It does not enable a listener or change the managed coordinator's runtime.
Preview is the default;
inspect the private proposed policy before an explicit `-apply`.

## Install and upgrade

Install the executable through an existing trusted distribution channel.
On Unix, mark the extracted binary executable (`chmod +x herdr-mesh`).
Herdr, Git, provider installation/authentication, and existing checkouts remain
independent prerequisites. Never put enrollment keys in an archive or copy an
enrolled tsnet identity to another node.

For managed `init`/`join`, keep the executable and its `herdr-mesh-state`
directory together in a writable, private local installation. CLI, dashboard,
and MCP share that computer's running connection; they do not need separate
enrollment or state. Use the same global `--state-dir` before commands if you
selected a different directory. Run `shutdown`, replace the binary in place,
then `start` to resume the saved identity. No login/boot startup is installed.

For advanced explicit roles, choose a persistent, private state directory for
each server, node, and client. An independently enrolled dashboard or concurrent
client needs separate state; two live tsnet processes cannot share an identity
directory.

Before an upgrade:

1. Identify the exact role/process and its state directory. Stop only that
   process, not every process with the same executable name.
2. Back up the complete stopped state directory, including database sidecars
   and identity material, to private storage. Do not copy a live SQLite file
   alone or export secrets to a shared project catalog.
3. Upgrade the coordinator before enabling node features that require new
   protocol capabilities. Replace the executable only after it has exited on
   Windows, or use a new versioned executable path.
4. Restart with the same state and verify actual connectivity and per-operation
   readiness. A restored inventory entry is stale until refreshed.

Do not downgrade across schema changes unless that exact migration path is
documented. Restore a complete consistent backup only with the corresponding
compatible binary and after accounting for effects that happened since the
backup: restoring old deduplication state can make a previously completed
operation look new. Backup restoration is not an automatic retry strategy.

## Uncertainty and journal capacity

Use `ctl command -id <known-id>` to inspect an existing receipt. Retry the same
operation only with its original idempotency key, payload, target identity,
and timing parameters. Input delivery, observed agent state, and semantic task
success are different facts.

Do not remove database rows, clear a journal, change project IDs, or generate
new request keys to bypass an indeterminate outcome or a capacity error.
Those actions can duplicate effects. Preserve the error and receipt, inspect
the actual target, and use the local `maintenance inspect`, `backup`, and
`verify-backup` commands described in [Offline journal maintenance](journal-maintenance.md)
for advanced per-role state. Those commands do not back up the shared managed
installation; preserve its complete state directory while stopped.
They preserve uncertainty and retry identities; they do not prune, force an
outcome, or automatically restore old execution state.

The mesh cannot contact a node process that is not running. Initial installation
and starting that process require an existing out-of-band administration path.
This release does not silently configure SSH, install an OS service, or run
unrestricted remote shell commands. Service managers can launch the documented
foreground roles with the same private persistent state; provider/headless
compatibility must still be verified for the chosen account and environment.
The embedded [Windows bootstrap command](node-bootstrap.md),
`herdr-mesh bootstrap`, can stage a verified local release using an existing
trusted SSH Host alias and prepared PowerShell endpoint, then optionally start
an explicitly matched existing Scheduled Task. It does not create that runner
or establish credentials. PowerShell 7.4+ and OpenSSH remain prerequisites.

An optional [coordinator-hosted dashboard](coordinator-dashboard.md) shares the
coordinator's enrolled identity and in-process inventory. The loopback dashboard
shares local IPC in managed mode; only explicit `dashboard --server` mode
requires its own enrolled client state.
