# GossipMesh Benchmarking Suite

## Standard SWIM + GossipMesh

The independent Go variant in [`chord_standard_swim/`](chord_standard_swim/README.md)
adds full-membership SWIM probing, suspicion/refutation, bounded piggyback gossip,
and a coordinator that schedules safe Chord repairs. The pure Chord baseline
below remains unchanged. The optimized finger-scoped variant is still separate
future work.

```sh
(cd chord_standard_swim && go build -race -o /tmp/chord-standard-swim .)
python3 -m orchestrator.standard_swim_verify --binary /tmp/chord-standard-swim --nodes 6
```

See the [implementation/run guide](chord_standard_swim/README.md) for configuration,
restart policy, tests and metrics, and the
[verification report](reports/standard-swim-verification.md) for actual results.
The baseline Docker runner below does not select the SWIM variant.

## Directory Layout
Matches the architecture defined in [gossipmesh_implementation_plan.md](gossipmesh_implementation_plan.md):

```text
├── baseline_chord/          # Case 1: Pure Chord (No SWIM) in Go
│   ├── Dockerfile
│   ├── main.go               # Entry point & background goroutines
│   ├── chord.go              # Ring, finger table, stabilize, successor list
│   ├── network.go            # RPC and HTTP overlay listener
│   ├── metrics.go            # Logging and telemetry
│   └── chord_test.go         # Unit tests
├── chord_standard_swim/     # Case 2: Chord + full-membership SWIM + GossipMesh
├── chord_optimized_swim/    # Case 3: Chord + Scoped SWIM (Folder prepared)
├── orchestrator/            # Central Python runner & Docker Compose
│   ├── runner.py             # Cluster management & fault injection
│   └── docker-compose.yml    # 8-node network definition
└── metrics/                 # Output CSVs and JSON telemetry
```

## Running the Docker Benchmark

### 1. Run the Orchestrator Benchmark (Docker Way)
```bash
python3 orchestrator/runner.py
```
This automatically:
1. Spawns an 8-node cluster connected via Docker network `chord-net`.
2. Tests **Healthy Lookups** and verifies correctness against ground-truth oracle.
3. Injects a **Gray Failure** using `tc qdisc netem delay 300ms` and records latency spikes.
4. Injects a **Hard Crash** via `docker stop`, measures routing recovery time, and checks post-crash lookups.
5. Collects telemetry (RPC counts, bytes transferred, memory allocations) and writes results to:
   - `metrics/baseline_benchmark_results.json`
   - `metrics/baseline_benchmark_results.csv`

The runner waits for oracle-checked predecessors, successors, successor lists,
and every finger before measuring a stable phase. Random finger refresh can take
longer than one sweep; the convergence deadline is five minutes. Recovery requires
at least 99% correct sampled owners across all survivors for two consecutive
batches. A recovery timeout is JSON `null` / CSV `TIMEOUT`, and
`verification_passed` records whether the healthy, recovery and post-crash checks
passed. This sampling criterion is not a proof about every possible lookup.

**The committed CSV/JSON results predate the paper-alignment fixes and must be
regenerated before use in comparisons.** Byte counters measure overlay body
payloads, excluding HTTP/TCP headers. Runtime allocation statistics cover the Go
process, not just Chord routing records.

### 2. Run Local Go Unit Tests
```bash
cd baseline_chord && go test -v ./...
```

For the expanded protocol and measurement checks, from the repository root:

```bash
(cd baseline_chord && go test -race -cover -timeout 240s ./... && go vet ./...)
python3 -m unittest orchestrator.test_runner -v
(cd baseline_chord && go build -race -o /tmp/gossipmesh-chord-fixed .)
python3 -m orchestrator.local_verify --binary /tmp/gossipmesh-chord-fixed
```

The local verifier starts eight real processes with accelerated maintenance,
checks every routing-table entry, validates sampled lookups, kills two adjacent
nodes, checks recovery, restarts them, and verifies convergence again. It cleans
up its processes and checks their logs for race reports and panics. It does not
exercise Docker or `tc netem`.

See [the paper-alignment audit](reports/go-chord-paper-audit.md) for the algorithm
mapping, fixes, verification results and limits. The implementation follows the
paper's Section 5 lazy maintenance, which replaces the eager join in Section 4.
It implements key-to-node routing, not storage, data migration or replication.
Use IDs in `[0, 2^M)` with `1 <= M <= 160`; peers must use the same M. Configure R
for the intended ring size/failure tolerance; the paper's probabilistic analysis
assumes a successor-list length proportional to log N. Arbitrary network
partitions and disjoint cycles have no automatic healing guarantee.

## Comparing Chord against Chord + standard SWIM

Use the **shared local comparison harness** for a paired experiment. It runs both
executables sequentially with the same IDs, Chord configuration, offered lookup
schedule, fault target, and independent owner oracle. It alternates execution
order across repetitions. The older Docker baseline and standalone SWIM verifier
results use different scenarios; do not compare their numbers directly.

```bash
(cd baseline_chord && go build -o /tmp/chord-compare-baseline .)
(cd chord_standard_swim && go build -o /tmp/chord-compare-swim .)
python3 -m orchestrator.compare_variants \
  --chord-binary /tmp/chord-compare-baseline \
  --swim-binary /tmp/chord-compare-swim \
  --repeats 3 --output reports/chord-swim-comparison.json
python3 -m unittest orchestrator.test_compare_variants \
  orchestrator.test_standard_swim_verify orchestrator.test_runner -v
```

Both variants export these phases with identical summary fields:

| Phase | Condition and oracle |
|---|---|
| `healthy` | Converged ring; all nodes are owners |
| `gray_failure` | One process stays alive; every incoming peer RPC to it is delayed; all nodes remain owners |
| `gray_recovered` | Delay removed and routing/membership reconverged before measuring |
| `crash_recovery` | Same process hard-killed; continuous scheduled lookups among survivors, using survivor ownership |
| `post_crash` | Surviving routing tables and membership converged |
| `restart` | Same identity/address restarted and reconverged; all nodes become owners again |

Output files are [the comparison table](reports/chord-swim-comparison.md),
[raw JSON](reports/chord-swim-comparison.json), and
[per-run CSV](reports/chord-swim-comparison.csv). JSON contains each request,
configuration, binary hashes, metric snapshots/deltas, peer traffic, and SWIM
structured events. CSV has one row per variant, repetition and phase; missing
measurements are blank. Both variants report oracle correctness separately from
RPC success, p50/p95/p99 latency, correct-lookup mean hops, peer requests/body
bytes, and whole-process Go heap. SWIM-only counters and membership entries are
`null` for the baseline, rather than invented zeros.

Defaults: four nodes, four-bit ID space, successor-list size three, Chord
stabilization/predecessor checks every 200ms, finger fixing every 100ms, Chord RPC
timeout 250ms; SWIM interval 200ms, direct timeout 50ms, two helpers, suspicion
1200ms. Four-second ordinary phases offer 24 queries/second; crash observation
lasts six seconds. Configure these with `--nodes`, `--m`, `--stabilize-ms`,
`--fix-ms`, `--phase-seconds`, `--recovery-seconds`, `--rate`, `--seed`, and
`--gray-delay-ms`. Use the same build flags for both binaries; the measured
comparison uses ordinary builds. For race checks, build **both** with `-race` in
a separate verification run, rather than comparing an instrumented binary with
an uninstrumented one.

The gray fault is a **100ms application-level forwarding delay** by default,
applied through a proxy to both Chord and SWIM peer RPCs. Client lookup and
management requests access the backend directly. The fault lasts the configured
phase duration; requests already delayed can finish after removal. This models a
slow reachable peer, not packet-level congestion, packet loss, or Docker netem.
The local proxy adds overhead to both variants. Proxy counters count each
request at its destination and request/response bodies once, excluding headers
and TCP/IP overhead; errors can include client disconnects. Metric snapshots
bracket each phase, so traffic includes background work and small scrape/drain
boundary effects. Heap snapshots exclude the Python proxy and are not
protocol-specific memory measurements.

Recovery requires at least 99% oracle correctness **per survivor** in two
consecutive one-second cohorts. Completion time is included: late replies cannot
backdate recovery. This conservative statistic has a two-second minimum and
cannot distinguish faster recovery below that resolution. Missing recovery is
`null`, never zero. The same seeded keys and offered schedules are used for each
paired phase; raw samples retain scheduling and completion times. In-flight
requests are bounded, and harness overload is recorded as a failed sample.
Routing convergence checks all fingers, neighbors and successor lists; lookup
samples themselves do not prove all-key correctness for arbitrary rings.

Three local repetitions provide descriptive medians and ranges, not statistical
proof that one protocol is better. Compare correctness, tail latency, recovery,
and traffic/memory overhead together. A fast Chord lookup fallback can recover
before SWIM confirms death, so SWIM detection and lookup recovery are different
measurements. No node implementation or existing baseline benchmark is changed
by this shared harness.
