# Tailnet setup through herdr-mesh

This is the advanced setup reference. The normal
[operator guide](operator-guide.md) uses guided `init` and browser-based `join`,
without asking you to count or distribute device keys.

A **Tailscale auth key** (also called an enrollment key here) is a secret
invitation for a mesh instance to join the private network. It is used for
initial connection, not for every command, agent, or restart. It is distinct
from an **API access token**, which grants permission to configure Tailscale.

`herdr-mesh setup tailnet` configures policy and keys through the Tailscale HTTPS
API in Go on Windows, Linux, and macOS. It needs no PowerShell or setup script.
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
permissions on an arbitrary existing directory. The output directory itself
cannot be a link or reparse point; installer-managed parent links are supported.
Review the proposal before choosing to apply. Apply fetches current
policy again and guards that update with its ETag; it does not submit the previous
preview file.

| Flag | Default / behavior |
| --- | --- |
| `-tailnet` | Required tailnet name |
| `-api-token-env` | `TAILSCALE_API_TOKEN`; names a `tskey-api-` access token, not a device enrollment key |
| `-tag-owner` | `autogroup:admin`; adds an owner without deleting existing owners |
| `-key-expiry-seconds` | `604800`; range `3600..7776000` |
| `-keys-per-role` | `2`; range `1..100`, for each of server/node/client |
| `-dashboard-port` | Absent; grants Tailscale policy `*` sources TCP access to the server tag on `1..65535`, excluding the mesh RPC port `50052` |
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

Policy application uses the fetched `If-Match` ETag. The merge preserves either
`tagowners` or `tagOwners` as returned by the API, and skips the policy POST
when the reviewed additions are already present. Failure after key creation
attempts revocation of every key whose ID was received, including when writing a
private key file fails. Cleanup removes only numbered files and aliases created
by this invocation. A local cleanup failure does not stop remaining revocation
attempts. Backups/proposals remain available for operator inspection.

## Failure and cancellation

The CLI never prints token values, key values, or raw API/error payloads. A
rejected policy update includes the HTTP status number, but not the response
body. Errors use bounded categories, including missing/wrong token,
missing ETag, policy update failure, and unconfirmed key revocation.

**An error or timeout after an apply request starts means remote effects may be
unknown.** Canceling the local request cannot undo an accepted remote API
request. The policy may remain applied even if newly created keys
were revoked; a response lost after key creation may leave an unknown active key.
Inspect the Tailscale admin console and private artifacts before retrying.
There is no automatic apply retry, rollback, key pruning, or success-shaped
fallback. Preview does not perform remote mutations, but may leave local files.

Guided `init` can reconcile its own pending policy apply with a read-only API
request on the next run. It compares the live policy with the unique saved
pre-apply backup and reviewed proposal. If the backup matches, it clears the
pending marker and presents a fresh preview and confirmation. If the proposal
matches, it records policy completion. Any other result retains the marker and
requires operator inspection. The completion receipt identifies the exact
apply artifacts so later policy cleanup ignores earlier failed attempts.

The native implementation bounds policy and key responses, stores artifacts in
private files, and never includes credentials or raw API responses in errors.
Go tests exercise policy merge, private artifacts, ETag protection, key cleanup,
and sanitized failures with mocked HTTPS requests. No live tailnet is required.

## Integration interfaces

The app router calls `app.RunSetup(ctx context.Context, args []string, streams IO)
error`, where `args` starts with `tailnet` (the outer `setup` token is removed).

`setup.Run` reads the named API token environment variable for advanced use;
`setup.RunWithToken` accepts the hidden prompted token for guided `init`.
Both use the same native setup logic and sanitized `setup.Error` codes.
