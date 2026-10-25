# Adaptive Chord maintenance experiment

This optional scheduler backs off Chord's periodic work during locally quiet
periods. SWIM retains its full membership view, probe schedule, version rules and
suspicion semantics. Chord lookup validation and successor selection are unchanged.
The implementation is experimental and disabled by default.

Three independent workers run stabilization, finger fixing and predecessor checks.
Each worker remains serialized; a slow operation does not block the other workers
or membership processing. With adaptation disabled they use the original tickers.

With adaptation enabled, the existing maintenance intervals are the fast intervals.
After each quiet window, each interval doubles up to its cap. Backoff requires:

- An unchanged local routing snapshot, compared by peer identity and address.
- Populated successor, successor list and finger references; a predecessor for a
  nonsingleton node; no invalid or confirmed-dead local reference.
- No dirty fingers, queued/in-flight coordinator repair, or SUSPECT member.

This condition does **not** prove mathematical finger correctness or global ring
convergence. Periodic work continues at bounded intervals to discover topology
changes, including joins that cannot be inferred from liveness alone.

Accepted membership events (including local refutation), routing changes, and
failed outgoing Chord RPCs restore fast scheduling. Cancellation during shutdown
is excluded. Each worker has a bounded wake channel; events coalesce and bring a
slow timer forward to its normal fast deadline. They do not launch a goroutine or
full stabilization per event. Confirmed-dead targeted repair remains immediate
through the existing coordinator and repairs the successor before dirty fingers.
SUSPECT does not become DEAD merely because maintenance accelerates.

| Environment variable | Default | Meaning |
|---|---:|---|
| `ADAPTIVE_MAINTENANCE` | `false` | Enable adaptive scheduling |
| `ADAPTIVE_STABLE_AFTER_MS` | `2000` | Quiet window per doubling step |
| `ADAPTIVE_MAX_STABILIZE_MS` | `5000` | Stabilization interval cap |
| `ADAPTIVE_MAX_FIX_FINGERS_MS` | `2000` | Finger maintenance interval cap |
| `ADAPTIVE_MAX_CHECK_PRED_MS` | `5000` | Predecessor check interval cap |

Caps must be at least the corresponding `STABILIZE_INTERVAL_MS`,
`FIX_FINGERS_INTERVAL_MS`, and `CHECK_PRED_INTERVAL_MS` when enabled. Existing base
interval defaults remain 200, 100 and 300 ms. Values above are experiment parameters,
not universal deployment recommendations.

`/metrics` exposes a `maintenance` object containing enabled state, fast/backoff
mode, current intervals, quiet duration and the most recent reason/blocker. SWIM's
existing telemetry counters include `maintenance_stabilize_runs`,
`maintenance_fix_fingers_runs`, `maintenance_check_predecessor_runs`,
`maintenance_fast` and `maintenance_backoff`. Structured mode-transition events
carry local identity, timestamp, reason and applicable intervals. The local state
observer runs every 100 ms and adds CPU/allocation cost; this is not a free scheduler.

Build and test:

```sh
cd chord_standard_swim
go test -race ./...
go vet ./...
go build -o /tmp/chord-swim-adaptive .
```

Run an individual node with the existing identity/listener/bootstrap variables:

```sh
ADAPTIVE_MAINTENANCE=true STABILIZE_INTERVAL_MS=1000 \
FIX_FINGERS_INTERVAL_MS=500 CHECK_PRED_INTERVAL_MS=1000 \
NODE_ID=0 M=8 PORT=8000 /tmp/chord-swim-adaptive
```

From the repository root, compare the same binary in fixed and adaptive modes:

```sh
(cd baseline_chord && go build -o /tmp/chord-compare-baseline .)
python3 -u -m orchestrator.adaptive_maintenance_study
python3 -m orchestrator.report_adaptive_maintenance
```

The runner uses three paired repetitions per mode, identical eight-second lookup
phases, 100 ms gray delay, one crash/restart, and a two-second client quiet period
after crash. It records all adaptive configuration and runtime metrics. It refuses
to overwrite existing study reports. Each mode has its own plain-Chord controls;
fixed/adaptive SWIM share one executable SHA256. No builds or tests should overlap
performance measurement. The resulting reports test this workload only; larger
rings, repeated churn, packet loss and equal-budget parameter sweeps remain separate
experiments. This implementation does not remove routine predecessor checks, change
the SWIM transport, or implement finger-scoped probing.
