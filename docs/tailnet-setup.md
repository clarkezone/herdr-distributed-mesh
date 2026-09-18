# Tailnet setup through herdr-mesh

This is the advanced setup reference. The normal
[operator guide](operator-guide.md) uses guided `init` and browser-based `join`,
without asking you to count or distribute device keys.

A **Tailscale auth key** (also called an enrollment key here) is a secret
invitation for a mesh instance to join the private network. It is used for
initial connection, not for every command, agent, or restart. It is distinct
from an **API access token**, which grants permission to configure Tailscale.

`herdr-mesh setup tailnet` contains the policy/key setup implementation. No source
checkout or separately distributed setup script is required. PowerShell is an
internal runtime prerequisite: `pwsh` on PATH, or Windows PowerShell on Windows.
On Linux/macOS, use PowerShell 7.3 or newer for private Unix file permissions.
The command does not install tools, enroll devices, or start mesh processes.

Provide a Tailscale **API access token** (`tskey-api-`) through your process
environment or secret manager as `TAILSCALE_API_TOKEN` (the default). To use
another variable, pass only its **name** with `-api-token-env`, never its value:

```powershell
herdr-mesh setup tailnet -tailnet example.com -output-directory C:\private\mesh-preview
herdr-mesh setup tailnet -tailnet example.com -output-directory C:\private\mesh-applied -apply
```

The first invocation is **PREVIEW**: it reads the policy, writes a private original
backup and merged proposal, and makes no policy/key mutations. Applying is always
explicit. Use a **different, dedicated output directory** for each invocation;
existing policy proposals and numbered key files are never overwritten. An
existing directory must already have private permissions; setup does not change
permissions on an arbitrary existing directory. Links/reparse points in its path
are rejected. Review the proposal before choosing to apply. Apply fetches current
policy again and guards that update with its ETag; it does not submit the previous
preview file.

| Flag | Default / behavior |
| --- | --- |
| `-tailnet` | Required tailnet name |
| `-api-token-env` | `TAILSCALE_API_TOKEN`; names a `tskey-api-` access token, not a device enrollment key |
| `-tag-owner` | `autogroup:admin`; adds an owner without deleting existing owners |
| `-key-expiry-seconds` | `604800`; range `3600..7776000` |
| `-keys-per-role` | `2`; range `1..100`, for each of server/node/client |
| `-dashboard-port` | Absent; explicitly grants client-to-server TCP access on `1..65535` |
| `-output-directory` | Windows: `%LOCALAPPDATA%\herdr-mesh\tailnet-setup`; Unix: the user configuration directory's `herdr-mesh/tailnet-setup` |
| `-apply` | False; explicit remote policy/key changes |
| `-timeout` | `2m`; positive and at most `30m` |
| `-json` | Safe structured report with mode, local artifact paths, key count, warning codes |

## Preserved policy and key safeguards

The merge preserves unrelated rules and owners. It adds exact node-to-server and
client-to-server TCP `50052` grants only when absent, plus the optional dashboard
grant. Repeated merges do not duplicate those exact grants. Each enrollment key
is single-use, non-ephemeral, preauthorized, and tagged for exactly one mesh role.

The original response is saved privately as `policy-before-<timestamp>-<nonce>.json`;
`policy-proposed.json` is also private. Applying JSON serialization can remove
HuJSON comments and formatting. **Existing wildcard allow rules remain in place:
adding role grants does not provide network isolation.** This remains a deliberate,
operator-reviewed scratch-tailnet workflow, not an automatic policy hardening tool.

Numbered `server-key-N.ps1`, `node-key-N.ps1`, and `client-key-N.ps1` artifacts
contain enrollment secrets. Primary aliases are created only if absent; existing
aliases are preserved. These protected artifacts are not an additional product
executable. Never paste their contents into command arguments, logs, or tickets.
Revoke unused keys and remove consumed key artifacts after enrollment.

Policy application uses the fetched `If-Match` ETag. Failure after key creation
attempts revocation of every key whose ID was received, including when writing a
private key file fails. Cleanup removes only numbered files and aliases created
by this invocation. A local cleanup failure does not stop remaining revocation
attempts. Backups/proposals remain available for operator inspection.

## Failure and cancellation

The CLI never prints token values, key values, or raw API/error payloads. Errors
use bounded categories, including missing PowerShell, missing/wrong token,
missing ETag, policy update failure, and unconfirmed key revocation.

**An error or timeout after an apply process starts means remote effects may be
unknown.** Killing the local PowerShell/SSH process tree cannot undo an accepted
remote API request. The policy may remain applied even if newly created keys
were revoked; a response lost after key creation may leave an unknown active key.
Inspect the Tailscale admin console and private artifacts before retrying.
There is no automatic apply retry, rollback, key pruning, or success-shaped
fallback. Preview does not perform remote mutations, but may leave local files.

The shared embedded-script host accepts only structured JSON options, invokes
PowerShell with fixed arguments, and protects temporary scripts/data. It bounds
options (256 KiB), assets (eight additional files, 2 MiB each, 16 MiB total), and
stdout/stderr (256 KiB each). It terminates its owned Windows job or Unix process
group on cancellation and removes only its own known temporary files. Unexpected
temporary files cause an explicit cleanup error, not recursive deletion.

For maintainers, `scripts/configure-tailnet.ps1` is only a developer wrapper over
the same embedded asset. `scripts/test-configure-tailnet.ps1` uses mocked APIs;
Go tests also exercise private host execution, cancellation, CLI parsing, and
the embedded setup flow with fake API functions. No live tailnet is required.

## Integration interfaces

The app router calls `app.RunSetup(ctx context.Context, args []string, streams IO)
error`, where `args` starts with `tailnet` (the outer `setup` token is removed).

Reusable host:

```go
scripthost.Run(ctx, scripthost.Request{
    Script: entryScript,             // param([string]$OptionsPath)
    Assets: map[string][]byte{...},  // basenames, excluding reserved names
    Options: typedOptions,           // JSON, never interpolated into script
    Timeout: 2 * time.Minute,
}) // (scripthost.Result{Output, Started}, error)
```

`Output` is bounded and returned only on successful process execution, but callers
must still validate its JSON schema and safe fields before displaying anything.
A successful process may return a structured workflow failure. `*scripthost.Error`
contains a sanitized `Code`; `Started` remains meaningful on failure. Consumers
performing remote changes must retain that uncertainty, not label remote effects
as canceled. The host provides UTF-8 output and no interactive stdin.
