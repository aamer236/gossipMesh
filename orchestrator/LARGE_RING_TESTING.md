# Running 16–64-node comparisons

This branch contains standard full-membership SWIM, optional adaptive Chord maintenance, and the local dashboard. It does **not** implement finger-scoped SWIM or the proposed validation-aware adaptive policy.

## Get and build

Requires Go 1.20+ and Python 3.11+ (standard library only). Linux is the tested host.

```sh
git clone --branch codex/large-ring-experiments https://github.com/aamer236/gossipMesh.git
cd gossipMesh
mkdir -p /tmp/gossipmesh-large
(cd baseline_chord && go build -o /tmp/gossipmesh-large/chord .)
(cd chord_standard_swim && go build -o /tmp/gossipmesh-large/swim .)
```

## Controlled benchmark (recommended)

The harness runs one variant at a time and alternates variant order across repetitions. Start with 16 nodes, then change `--nodes` to 32 and 64 and use distinct output filenames. Keep `--m 8`: it provides 256 distinct identifiers. The parser rejects rings larger than the identifier space.

```sh
python3 -m orchestrator.compare_variants \
  --chord-binary /tmp/gossipmesh-large/chord \
  --swim-binary /tmp/gossipmesh-large/swim \
  --nodes 16 --m 8 --rate 64 --repeats 3 \
  --phase-seconds 30 --recovery-seconds 30 --convergence-seconds 300 \
  --stabilize-ms 1000 --check-pred-ms 1000 --fix-ms 500 \
  --rpc-timeout-ms 1000 --swim-interval-ms 1000 --swim-direct-ms 250 \
  --swim-suspicion-ms 5000 --crash-quiet-ms 2000 \
  --recovery-window-ms 1000 --swim-adaptive-maintenance \
  --output reports/large-16-adaptive.json
```

Repeat without `--swim-adaptive-maintenance` and write `large-16-fixed.json` to test fixed-maintenance SWIM. Repeat both modes at 32 and 64 nodes. A rate of 64 is **total offered lookups per second**, not per node; it satisfies the per-cohort coverage requirement through 64 nodes. Holding total load fixed and holding per-node load fixed answer different questions; record which you use. The two-second quiet period measures background repair before lookups resume. Repeat with `--crash-quiet-ms 0` for continuous client traffic.

JSON, CSV and Markdown reports are generated from the actual run, with binary hashes, configurations and acceptance results. Retain failed runs too. Report recovery censoring, false death events and harness overloads; do not silently discard them or interpret overload as a protocol failure. The gray fault injects incoming application delay, not packet loss. No large-ring performance claim follows from simply accepting the configuration.

## Interactive dashboard

```sh
python3 -m visualizer.dashboard.server
```

Open http://127.0.0.1:8787 and choose 16, 32 or 64 nodes. The range remains 2–64 for small debugging runs. Both rings run concurrently, so 64 nodes means **128 Go processes**, plus the Python proxy/monitor threads. Large-ring diagrams use small dots; use the node selector to inspect IDs, fingers and membership. Allow up to five minutes for initial convergence. Stop before changing ring size. Export before starting another experiment.

The dashboard is for exploration. Host scheduling, proxy overhead, management polling and simultaneous rings affect its measurements. Use the sequential benchmark for performance comparisons. Monitor host CPU, memory and open-file limits when scaling; do not run builds, race tests or other experiments during timing. If the host saturates, reduce load or move to a larger host and report that limitation.

## Tests

```sh
(cd baseline_chord && go test -race ./...)
(cd chord_standard_swim && go test -race ./...)
python3 -m unittest orchestrator.test_compare_variants visualizer.dashboard.test_dashboard -v
# First run the dashboard once to build its default /tmp binaries.
DASHBOARD_E2E=1 DASHBOARD_TEST_NODES=16 python3 -m unittest visualizer.dashboard.test_dashboard -v
# Optional larger lifecycle smoke tests (not performance benchmarks):
DASHBOARD_E2E=1 DASHBOARD_TEST_NODES=32,64 python3 -m unittest visualizer.dashboard.test_dashboard -v
```

The lifecycle smoke tests check startup, sampled healthy oracle correctness and process cleanup at the chosen sizes. The default 2,4,8 suite also tests a four-node crash/restart and delay/heal. Use the benchmark command above to exercise actual failure phases at larger sizes.

Large-ring dashboard runs use a 1 s probe interval, 250 ms direct timeout, 5 s suspicion timeout and 1 s Chord RPC timeout. Small-ring settings are unchanged. These conservative starting settings trade detection speed for tolerance of local host scheduling delays; tune them only with false-suspicion and overhead measurements. The initial 16-node smoke run with the old 50 ms direct timeout produced repeated false suspicions under local load and was interrupted.

## Verification in this branch

- 21 benchmark regression tests passed, including 16/32/64 configuration, unique IDs, and configurable timeout validation.
- Python compilation and JavaScript syntax checks passed.
- The paired 16-node dashboard smoke test with the fast Chord maintenance profile and revised probe timeouts **failed its 300-second startup convergence requirement**. A direct oracle inspection near the deadline found the SWIM ring fully converged, while plain Chord retained incorrect fingers (including immediate-successor fingers). No timed lookup workload started. Cleanup completed. This is an unresolved large-ring startup limitation, not a successful benchmark or evidence of a performance win.
- 32- and 64-node process runs have not been verified here. They are enabled for experimentation, not certified correct at those sizes. The documented benchmark uses slower balanced maintenance; its large-ring outcomes remain to be measured.
- A fresh Go race-test invocation was declined at the permission prompt. Earlier adaptive-source race results remain in the saved study; no new race-test success is claimed for this publication.

If startup fails, retain the error report and distinguish this from recovery latency. Do not bypass the oracle to manufacture a benchmark result. The baseline implementation has not been modified to hide this failure.
