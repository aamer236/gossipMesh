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

## Demonstrating SWIM-triggered Chord repairs

1. Start four nodes and wait for **Running**. For a presentation, inspect node 0's fingers before crashing node 128.
2. In **Node routing and finger tables**, choose any observer node. Each row shows the zero-based finger index, `(node ID + 2^index) mod 2^M`, its owner in the selected episode's last collected pre-crash snapshot, current owner, and an independent current-owner oracle. Changed rows are highlighted; hover the owner to see its address.
3. Crash node 128 using the paired fault controls. In **SWIM → Chord failure evidence**, select the crash episode. The stages show only observed source events: suspicion, death, coordinator receipt, affected references, dirty fingers and validated repair events. The table includes the surviving observer ID and the Go event's source timestamp.
4. Match a `finger_repaired` event's `local_id`, `index`, `start` and `owner` to that observer's finger row. This demonstrates that the membership-driven coordinator executed that repair. `references_identified` shows the affected successor/predecessor/list/fingers. `successor_repair_validated` records validated successor repair when that path is needed.
5. An unobserved stage is not fabricated or treated as a failure: ordinary maintenance or lookups can repair references first; unrelated observers may have nothing to repair. A completed job may have zero repaired references. Changed routing rows alone do not attribute cause to SWIM.
6. Export the run before starting another experiment. Up to 40 fault records are retained, each crash episode retaining at most 256 relevant events plus its pre-crash snapshots. Any later fault action closes collection for the previous episode; existing evidence remains available. The general event stream has its separate bound.

Snapshots are sampled, not atomic with process termination. Before-values mean the last collected pre-crash snapshot. Source event timestamps and dashboard collection times differ; events from different observers may be collected in a different order. Crashed nodes have no current live snapshot. Restarting a node starts a new membership episode and does not rewrite the earlier crash's evidence.

## Put/Get storage

The **Store and retrieve the same record** panel performs real paired Put/Get operations through Chord into each owner Go process's memory. See [storage behavior, API and limitations](STORAGE.md). This first version has no replication, persistence or key migration; storage availability must not be confused with routing recovery.

New experiments automatically preload a shared random set of records after convergence. The **Keys stored by each node** table compares the actual Go-process inventories; **Inspect** opens values and fingers. Export includes the random seed and both variants' actual write replies. Restart does not refill lost records.
