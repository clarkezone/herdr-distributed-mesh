# Windows production transport validation

This is the remaining Phase 1 transport gate. It is deferred while the read-only
Herdr integration proceeds, but must be completed before the two-machine demo.
Run the production `herdr-mesh` command, not the disposable spike, on two Windows
hosts. See the README's Herdr connection section for local use.

## Tailnet prerequisites

Create separate enrollment credentials authorized for these tags:

- Server: `tag:herdr-mesh-server`
- Node: `tag:herdr-mesh-node`
- Controller and doctor: `tag:herdr-mesh-client`

Tailnet policy must allow the node and client tags to reach the server tag on
TCP port `50052`. Do not use a server- or client-capable enrollment credential
on a node. Application-layer authorization based on Tailscale `WhoIs` is the
security boundary for role separation while any wildcard network ACL remains.

The production topology uses embedded tsnet for every role. A separately
installed Windows Tailscale client is not required and is outside this
hackathon gate.

Automate the policy and key setup from an administrator workstation:

```powershell
$env:TAILSCALE_API_TOKEN = '<tskey-api-access-token>'
.\scripts\configure-tailnet.ps1 -Tailnet 'example.com'
```

Use an API access token generated from the Tailscale admin console Keys page.
Device enrollment auth keys beginning with `tskey-auth-` cannot call this API.
The script trims surrounding whitespace and rejects the wrong key type before
making a request.

The script backs up the untouched current policy, merges only the role tag
owners and TCP `50052` grants using ETag protection, then creates two distinct
one-off keys per role by default. Use `-KeysPerRole <count>` when the validation
plan needs a different number. It stores predictable numbered files such as
`server-key-1.ps1` and `server-key-2.ps1` under
`$env:LOCALAPPDATA\herdr-mesh\tailnet-setup` with an ACL restricted to the
current Windows user. Convenient `server-key.ps1`, `node-key.ps1`, and
`client-key.ps1` aliases load key 1, but are created only when those files do
not already exist. The script refuses to proceed if a numbered destination
already exists, so an unconsumed key is never overwritten. Use `-WhatIf` to
preview the merged policy without changing the tailnet or creating keys.

**Scratch-tailnet warning:** policy round-tripping serializes the policy as JSON
and removes HuJSON comments and original key formatting. This is intentional
for the scratch/hackathon tailnet workflow. The timestamped
`policy-before-*.json` file preserves the exact response for review or
restoration. The script also warns prominently if it detects an existing
wildcard allow ACL: the added role grants do not provide network isolation in
that case, and the script does not delete or narrow existing rules.

Each key is non-reusable and expires after the configured period (seven days by
default), but its file remains a secret until consumed or revoked. After each
enrollment, clear the corresponding `TS_AUTHKEY_*` environment variable and
securely delete the consumed numbered key file. Revoke unused keys in the
Tailscale admin console before expiry, and securely remove all remaining key
files when validation ends.

The local mocked API test does not require a Tailscale credential:

```powershell
.\scripts\test-configure-tailnet.ps1
```

The setup and test scripts support both Windows PowerShell 5.1 and PowerShell
7. The API read uses basic parsing to avoid the Windows PowerShell web-content
execution prompt.

## Build

Run on both hosts:

```powershell
go build -o .\herdr-mesh.exe .\src\cmd\herdr-mesh
```

## Start the server

```powershell
$env:TS_AUTHKEY_SERVER = '<server-role-auth-key>'
.\herdr-mesh.exe server `
  -hostname herdr-mesh-server `
  -state-dir "$env:LOCALAPPDATA\herdr-mesh-validation\server"
```

After first enrollment, remove `TS_AUTHKEY_SERVER` from the environment. Retain
the state directory for restart tests.

## Start the node

Use the server's full MagicDNS name:

```powershell
$env:TS_AUTHKEY_NODE = '<node-role-auth-key>'
.\herdr-mesh.exe node `
  -hostname "herdr-mesh-node-$env:COMPUTERNAME" `
  -state-dir "$env:LOCALAPPDATA\herdr-mesh-validation\node" `
  -server 'herdr-mesh-server.example.ts.net:50052'
```

The server should log a successful Hello/HelloAck negotiation followed by
heartbeats from a stable node instance ID.

## Run doctor

From the second host or a separate client state directory:

```powershell
$env:TS_AUTHKEY_CLIENT = '<client-role-auth-key>'
.\herdr-mesh.exe doctor `
  -state-dir "$env:LOCALAPPDATA\herdr-mesh-validation\doctor" `
  -server 'herdr-mesh-server.example.ts.net:50052'
```

Capture the output. It reports:

- Embedded tsnet stable node ID and MagicDNS name
- Tailnet addresses
- Requested and assigned tags
- Node-key expiry
- tsnet state directory and health
- Server implementation and compatible protocol range

For machine-readable evidence:

```powershell
.\herdr-mesh.exe doctor `
  -json `
  -state-dir "$env:LOCALAPPDATA\herdr-mesh-validation\doctor" `
  -server 'herdr-mesh-server.example.ts.net:50052' |
  Set-Content .\doctor-result.json
```

Do not commit `doctor-result.json`; it contains tailnet identifiers and
machine-local state.

## Required test matrix

| Test | Procedure | Pass condition |
|---|---|---|
| Role tags | Run node and doctor with their normal role states | Server accepts both and doctor reports the expected assigned tag |
| Wrong role | Run `ctl server-info` from a separate node-tagged test identity as shown below | Server rejects the request with `PermissionDenied` |
| Server restart | Stop and restart the server using the same state directory | Stable IDs remain unchanged and the node reconnects |
| Node restart | Stop and restart the node using the same state directory | The server sees the same mesh and Tailscale identity |
| Client-before-server | Start the node before the server | The node retries without a tight loop and registers after the server starts |
| Network change | Switch the node between expected Windows networks | The stream recovers without re-enrollment |
| Sleep/wake | Sleep and resume a host where practical | The stream recovers and stable identities remain unchanged |
| Direct address | Run doctor using the server's tailnet IP instead of MagicDNS | Protocol compatibility check succeeds |
| Re-enrollment collision | Enroll a second, separate server state using the same requested hostname | Record assigned MagicDNS names; durable identity must not rely on hostname alone |
| Hackathon disruption loop | Repeat server restart, node restart, and network interruption three times over at least 10 minutes | Every disruption recovers without re-enrollment or identity change |

Never point two running processes at the same tsnet state directory. For the
re-enrollment test, use a new directory rather than deleting or copying an
existing identity.

For the wrong-role test, consume a second node key and use a dedicated state
directory. Never reuse the production node state:

```powershell
$env:TS_AUTHKEY_NODE = '<second-node-role-auth-key>'
.\herdr-mesh.exe ctl server-info `
  -auth-key-env TS_AUTHKEY_NODE `
  -tags 'tag:herdr-mesh-node' `
  -state-dir "$env:LOCALAPPDATA\herdr-mesh-validation\wrong-role-node" `
  -server 'herdr-mesh-server.example.ts.net:50052'
```

The command must fail with server `PermissionDenied`. Clear
`TS_AUTHKEY_NODE` and securely delete the consumed key file afterward.

## Evidence to retain

Record the following in the project delivery system, not shared AI Core memory:

- Date and application version or commit
- Windows versions for both hosts
- Assigned stable IDs, MagicDNS behavior, and role tags
- Restart/reconnect timings
- Network and sleep/wake observations
- Disruption-loop duration and unexplained disconnect count
- Final go/no-go result for the two-machine demo

Machine-specific paths, credentials, device state, and current run status must
remain outside shared AI Core memory.

## Headless agent-control compatibility

The initial existing-agent slice was exercised on Windows with Herdr
`0.7.5-preview.2026-07-29-44b3adb12552` (protocol 18) and a real Copilot agent.
The experiment cold-started a disposable named `herdr ... server` with isolated
configuration and a disposable Git directory. **No frontend ever attached.**
Workspace creation, agent discovery, a warm prompt with an identifiable response,
terminal reads, state waits, and actual provider cancellation worked headlessly.
This local proof does not replace the two-host mesh/disruption gate above.

Three compatibility findings affect interpretation and the later launch slice:

1. With PowerShell as the shell, `agent start` with an empty argument vector
   timed out because Herdr generated `Start-Process -ArgumentList ''`.
   After verifying no agent existed, a launch with Copilot's supported
   `--no-auto-update` argument succeeded. This is a launch compatibility issue,
   not permission to blindly retry uncertain starts.
2. An immediate post-launch prompt returned an idle acknowledgement while
   Copilot was still loading, without a task response. A distinct prompt after
   startup settled produced the expected response. Ready/idle transitions and
   native integrated waits are not semantic task-completion evidence.
3. One Escape showed "esc again to interrupt" and a transient Herdr idle state,
   but the requested text task still completed. Two Escape keys in **one**
   `agent.send_keys` request produced "Cancelling", then the explicit provider
   output "Operation cancelled by user", without the second task's response.
   This is the basis for the Copilot interrupt mapping. A concurrent Herdr wait
   did not block input through another connection.

Keep live experiments confined to an explicitly disposable session. Never
attach a terminal to compensate for a headless failure, issue prompts into a
user's active agent, or remove shared session/state roots during cleanup.

### Local mesh round-trip

The opt-in Windows fixture also passed against that real headless Copilot
instance: the real node, coordinator, adapter, and both SQLite journals carried
a prompt and its actual response, GET/READ/WAIT, wait cancellation, and a durable
interrupt receipt. It reopened journals and checked that read output was not
persisted. The gRPC link uses local in-memory transport with test identities;
this is not a new cross-host tsnet or production-deployment claim.

To reproduce, supply an already-running **disposable, idle Copilot** session:

```powershell
$env:HERDR_MESH_AGENT_TEST_SOCKET = '<disposable Herdr socket marker path>'
$env:HERDR_MESH_AGENT_TEST_PANE = '<pane-id>'
go test .\src\internal\server -run '^TestAgentLiveHeadlessQueryAndDurableInterrupt$' -count=1 -timeout=120s
Remove-Item Env:\HERDR_MESH_AGENT_TEST_SOCKET
Remove-Item Env:\HERDR_MESH_AGENT_TEST_PANE
```

The fixture sends a fresh text-only prompt and an interrupt; it is skipped
without the socket environment variable. It requires a nonce-bearing response
that cannot match the prompt echo or old output. The response marker is short
enough to avoid Copilot's application-level line wrapping: `recent_unwrapped`
does not undo wrapping already introduced by the provider's layout.
