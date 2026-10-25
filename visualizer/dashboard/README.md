# GossipMesh live lab

From the repository root, with Go and Python 3 installed:

```sh
python3 -m visualizer.dashboard.server
```

Open http://127.0.0.1:8787. The server builds both current Go implementations into `/tmp/gossipmesh-dashboard-bin`. No Docker or frontend dependencies are needed. Use `--port` to change the port; `--skip-build` reuses those binaries.

Choose 2–64 nodes, a common base maintenance profile and lookup rate, then **Start both rings**. Adaptive maintenance applies only to the SWIM variant. Wait for convergence, inject a paired **Crash**, and wait for both recovery measurements before **Restart**. Delay adds 100 ms to incoming peer requests; **Clear delay** removes it. A running delayed node remains in the live-node oracle. Stop closes owned processes and proxy listeners; Ctrl-C also shuts down the experiment. Closing the browser alone does not stop it.

## What the measurements establish

- Both rings use identical IDs, offered keys, initiators and fault actions. The independent lookup oracle checks the returned owner ID and address against the mathematical successor among processes known alive by the controller. A response spanning a fault is excluded and counted in export totals.
- Correctness and p95 use completed observations in the last 10 seconds. Timeout durations are included when available. Export includes harness overload and exclusion counts; overload is not evidence of protocol failure.
- Peer requests and payload body bytes are counted at the proxies. These include Chord and SWIM traffic, but exclude HTTP/TCP/IP headers, management polls and direct client requests. Forwarded lookup RPCs are included.
- Full-routing recovery requires two consecutive snapshots with correct successors, predecessor references, successor lists and all fingers, nominally 1 second apart. This is an observed upper bound affected by polling. Client lookups continue and may themselves repair references. Lookup availability and full-routing recovery are different measurements.
- Subsequent faults interrupt an unfinished recovery measurement. An unobserved time is not infinity. Delay/heal actions do not measure topology recovery.
- Ring lines show reported immediate successors. Red nodes are controller-known crashed processes; amber nodes have injected delay. The node inspector separately shows SWIM's local membership belief.
- The two rings run concurrently on one host. This is an exploratory dashboard, not an isolated statistical benchmark or a claim that either variant always wins. Small-ring survivor/rejoin limitations remain those of the real implementations.

Export downloads settings, binary hashes, cumulative counters, recent lookup samples, event records, fault recovery observations and chart history. Memory histories are bounded (2,400 samples per variant, 600 chart observations, 240 events, 40 faults). Process logs are temporary and removed on stop. Save exports before starting a new experiment.

## Verification

```sh
python3 -m unittest visualizer.dashboard.test_dashboard -v
# After the server has built the binaries:
DASHBOARD_E2E=1 python3 -m unittest visualizer.dashboard.test_dashboard -v
node --check visualizer/dashboard/app.js
```

The opt-in test starts real 2-, 4-, and 8-node paired rings, checks healthy ownership, exercises crash/recovery/restart and delay/heal on the four-node ring, and verifies process cleanup. Baseline Go code is reused without modifications.

For 16–64-node settings and sequential benchmark commands, see [large-ring testing](../../orchestrator/LARGE_RING_TESTING.md). Large rings use compact dots; select a node to inspect its ID and routing state.
