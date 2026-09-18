# Coordinator-hosted read-only dashboard

The coordinator can host the existing dashboard directly on its embedded
Tailscale network. It shares the coordinator's inventory, assets, and enrolled
identity; it does not create another controller identity or execution engine.
The default remains disabled.

```powershell
herdr-mesh server `
  -dashboard-listen :8787 `
  -dashboard-origin 'http://mesh-server.example.ts.net:8787'
```

The listener is tsnet-only, never a wildcard host-network HTTP listener.
These are alternative coordinator startup commands, not concurrent servers.
Stop and restart the intended coordinator using the same state; if it originally
used `-state-dir`, keep that override. Substitute its actual assigned full
MagicDNS name in the origin.
The exact origin is required; its port must match the dashboard listener, and
the dashboard and gRPC ports must differ. Host aliases and cross-origin requests
are rejected. The dashboard stays read-only, including its HTTP API.

Every page, asset, and API request authenticates the actual connection peer
through the same tsnet network. A stable peer identity and the coordinator's
`-required-client-tag` are required. Forwarded headers are not identity sources.
Do not expose this endpoint through an unauthenticated reverse proxy: doing so
would replace the real peer with the proxy.

The browser machine needs existing Tailscale connectivity and the client role,
plus network reachability to the chosen port. `herdr-mesh setup tailnet
-tailnet '<tailnet-name>' -dashboard-port 8787` can include the narrow
client-to-server grant in its proposed policy. Preview is the default; inspect
the private proposal before an explicit `-apply`. Execution-node tags
alone do not authorize dashboard access. This is the existing transport-role
trust model, not a new per-project permission system.

For browser TLS, use the coordinator's **full Tailscale DNS name** and prepare
Tailscale HTTPS/certificate support separately:

```powershell
herdr-mesh server `
  -dashboard-listen :443 `
  -dashboard-origin 'https://mesh-server.example.ts.net'
```

HTTPS uses tsnet's TLS listener. Missing certificate prerequisites cause TLS
handshakes to fail; the mesh does not silently enable HTTPS, publish a public
endpoint, install certificates, or weaken peer checks. Plain HTTP remains
inside Tailscale's encrypted transport, but is not browser-level TLS.

gRPC and the hosted dashboard share a lifetime. An unexpected exit of either
stops the other and reports an error; shutdown waits for both before closing
network and state. Request time, concurrency, output size, and peer lookups are
bounded.

The standalone `herdr-mesh dashboard` command is unchanged: it binds only to a
literal loopback address and uses a separate client state directory. Neither
mode adds mutation controls, stores terminal transcripts, or interprets a
delivery receipt or an observed `done` state as semantic task success.
