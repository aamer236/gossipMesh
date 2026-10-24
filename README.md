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

### 2. Run Local Go Unit Tests
```bash
cd baseline_chord && go test -v ./...
```
