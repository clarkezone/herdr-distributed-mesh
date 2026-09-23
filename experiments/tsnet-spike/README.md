# Archived Windows tsnet feasibility experiment

This disposable developer experiment established whether two Windows processes
could embed Tailscale and exchange bidirectional gRPC messages without an
installed Tailscale daemon. It is not a production CLI, is not packaged, and is
not part of the operator workflow. Production uses only `herdr-mesh`.

The source is retained as transport evidence, outside `src\cmd`. It contains no
Herdr integration or production durability guarantees. Use only disposable
development enrollment and separate private state if deliberately rerunning it.
Never reuse production role identities or put keys in source or command lines.

From the repository root, a developer can run its local protocol checks:

```powershell
go test ./experiments/tsnet-spike
```

The experiment's server and client modes use port `50051`; the production mesh
uses `50052`. A real feasibility rerun requires two machines and explicit
development enrollment, restart, and disruption checks. Do not add these modes
to production releases or the operator manual.
