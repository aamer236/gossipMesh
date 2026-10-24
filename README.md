# GossipMesh Benchmarking Suite

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
├── chord_standard_swim/     # Case 2: Chord + Global SWIM (Folder prepared)
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
