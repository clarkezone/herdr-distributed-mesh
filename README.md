# herdr-distributed-mesh

Distributed mesh support for Herdr.

## Production foundation

The production application is one binary with explicit process roles:

```powershell
go run ./src/cmd/herdr-mesh server
go run ./src/cmd/herdr-mesh node -server '<server-magic-dns-name>:50052'
go run ./src/cmd/herdr-mesh ctl server-info -server '<server-magic-dns-name>:50052'
go run ./src/cmd/herdr-mesh doctor -server '<server-magic-dns-name>:50052'
```

`server` and `node` are long-running processes. `ctl` and `doctor` are
short-lived clients. Each role uses a separate persistent local state directory
by default. Initial enrollment uses a role-specific environment variable:
`TS_AUTHKEY_SERVER`, `TS_AUTHKEY_NODE`, or `TS_AUTHKEY_CLIENT`. Server/controller
credentials must not be distributed to nodes.

The server verifies every caller through Tailscale `WhoIs` and requires
`tag:herdr-mesh-node` for node streams and `tag:herdr-mesh-client` for control
requests by default. These tags must be granted to the corresponding enrollment
keys in the tailnet policy. The production listener defaults to port `50052`,
keeping it separate from the disposable spike on `50051`.

Use `docs\windows-live-validation.md` for the two-host production transport
gate, including automated tailnet setup, role rejection, restart, network
change, direct-address, MagicDNS re-enrollment, and a short hackathon disruption
loop.

The versioned protocol source is
`api\proto\agentflow\v1\control.proto`. Regenerate Go bindings with:

```powershell
winget install --id Google.Protobuf --exact
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.1
.\scripts\generate-proto.ps1
```

## Windows tsnet feasibility spike

The first milestone is intentionally disposable. It tests whether two Windows
processes can embed Tailscale with `tsnet` and exchange bidirectional gRPC
messages without a separately installed Tailscale daemon. It does not contain
Herdr integration or production mesh architecture.

Use a development Tailscale auth key that may enroll both nodes, or separate
keys. Keep keys in environment variables and never commit them.

On the server machine:

```powershell
$env:TS_AUTHKEY_SERVER = '<development-auth-key>'
go run ./src/cmd/spike server `
  -hostname herdr-mesh-spike-server `
  -state-dir "$env:LOCALAPPDATA\herdr-mesh-spike\server"
```

After the server is enrolled, use its full MagicDNS name on the client machine:

```powershell
$env:TS_AUTHKEY_CLIENT = '<development-auth-key>'
go run ./src/cmd/spike client `
  -hostname herdr-mesh-spike-client `
  -state-dir "$env:LOCALAPPDATA\herdr-mesh-spike\client" `
  -server 'herdr-mesh-spike-server.example.ts.net:50051'
```

Each line entered by the client is sent through a bidirectional gRPC stream and
returned as a pong. If the stream fails, the client retries the same message
until the server becomes reachable or Ctrl+C is pressed.

The state directories contain persistent tsnet node identities. Keep the server
and client directories separate, do not share them between machines, and retain
them when testing restart behavior. If the state is already enrolled, the auth
key environment variable may be omitted.

Optional flags:

```text
-auth-key-env <name>  Environment variable containing the auth key
-debug                Enable verbose tsnet logs
-listen <address>     Server listener, default :50051
-retry-delay <value>  Client reconnect delay, default 2s
-tags <tag:a,tag:b>   Tailscale tags requested during enrollment
```

Run the local protocol test:

```powershell
go test ./src/cmd/spike
```

The actual feasibility decision requires the server and client to run on two
Windows machines on the same tailnet, followed by restart and temporary network
loss tests.

That feasibility test has passed. The spike remains isolated from production
packages so it can be removed after the production transport is validated.
