# Windows production transport validation

This is the remaining Phase 1 gate before Herdr integration. Run the production
`herdr-mesh` command, not the disposable spike, on two Windows hosts.

## Tailnet prerequisites

Create separate enrollment credentials authorized for these tags:

- Server: `tag:herdr-mesh-server`
- Node: `tag:herdr-mesh-node`
- Controller and doctor: `tag:herdr-mesh-client`

Tailnet policy must allow the node and client tags to reach the server tag on
TCP port `50052`. Do not use a server- or client-capable enrollment credential
on a node.

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

The script backs up the current policy, merges only the role tag owners and
TCP `50052` grants using ETag protection, then creates one-off role-scoped auth
keys. It stores the keys under
`$env:LOCALAPPDATA\herdr-mesh\tailnet-setup` with an ACL restricted to the
current Windows user. Use `-WhatIf` to preview the merged policy without
changing the tailnet.

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
| Wrong role | Run doctor from a node-tagged identity | Server rejects the Fleet request with permission denied |
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

## Evidence to retain

Record the following in the project delivery system, not shared AI Core memory:

- Date and application version or commit
- Windows versions for both hosts
- Assigned stable IDs, MagicDNS behavior, and role tags
- Restart/reconnect timings
- Network and sleep/wake observations
- Disruption-loop duration and unexplained disconnect count
- Final go/no-go result for beginning Herdr integration

Machine-specific paths, credentials, device state, and current run status must
remain outside shared AI Core memory.
