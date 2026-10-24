# Standalone Chord: end-to-end verification

Date: 24 October 2026. Scope: one process, deterministic local transport and
simulation. No Docker, new networking, SWIM or GossipMesh integration was added.
Existing uncommitted visualizer/report work and nested repository were preserved.

## Audit and fixes

The [source-to-paper audit](chord-paper-audit.md) maps every selected mechanism to
the original SIGCOMM 2001 paper. Section 5 lazy join/stabilization is implemented;
Section 4 eager init_finger_table/update_others remains separate future work.
SHA-256/domains, zero-based fingers, deterministic repair, bounded lookup and
conservative failure behavior are explicit deviations/extensions. No claim is
made to every optimization or to the paper's full probabilistic failure theorems.

Two reproduced defects were fixed after adding failing regression tests:

1. Rejoin retained previous-lifecycle availability exclusions during bootstrap.
   New join now clears old hints before contacting an explicitly chosen peer.
   The remaining ring must still be reachable enough to complete joining.
2. Failed finger lookup did not advance its cursor, starving other entries.
   Cursor rotation now proceeds after index validation even when lookup fails;
   the old entry remains, and a success event is emitted only after actual repair.

Explicit simulator rejoin also makes a withdrawn/offline source available before
bootstrap RPCs. Crash recovery and withdrawal/rejoin remain distinct operations.

## Files and architecture

- `chord/node.py`: small lifecycle and cursor-fairness fixes.
- `simulator/clock.py`: cancellable virtual events, FIFO ties and finite budgets.
- `simulator/transport.py`: seeded/request-specific drops, directed RPC service
  delays/timeouts, extending existing in-memory delivery without sockets.
- `simulator/oracle.py`: independent sorted-ID ownership observer.
- `simulator/ring.py`: scheduling, node orchestration, faults and diagnostics.
- `simulator/__main__.py`: reproducible join/lookup/crash/repair/restore demo.
- `experiments/standalone.py`: multi-seed, phase-separated metrics and raw JSON.
- `experiments/scaling.py`: old interface retained; correctness ratio explicitly
  calculated instead of emitted as a constant after convergence.
- Expanded routing/failure tests; new simulator/benchmark tests; updated
  README, context, package discovery and strict type-check scope.

Storage is deliberately omitted: verifying routing does not require introducing
value migration, replication or durability claims. No concrete failure detector
or GossipMesh policy was added. Existing typed events/repair hooks are retained.

## Actual verification

Baseline: 39 tests passed in 10.521s, with clean format/lint/mypy/JS/Git checks.
The two defect regressions then failed (one assertion, one join error) before fixes.

Final commands actually run:

```sh
python3 -m unittest discover -q
.venv/bin/ruff format --check chord simulator tests experiments visualizer
.venv/bin/ruff check chord simulator tests experiments visualizer
.venv/bin/mypy
node --check visualizer/static/app.js
git diff --check
python3 -m simulator
```

**61 tests passed in 12.567s.** All 27 Python files were formatted, lint passed,
strict mypy passed for 17 source files (including experiment CLIs), and JS
syntax/Git whitespace checks passed. Additional tests verify memory tracing is
cleaned up after invalid workloads and cancelled events do not exhaust a live
callback budget. The latter regression initially failed and was fixed before
this final run. Pending live events remain queued on budget exhaustion.
The demo restored all 32 nodes with zero incorrect fingers, successors,
predecessors or successor lists. Tests require no sleeps or sockets.

New coverage includes every lookup from every node across all 255 nonempty
membership subsets of an eight-ID space, 24 sequential join permutations,
correct successor routing with stale live fingers, queued failures/recovery,
deterministic drops/delays, directed links, lifecycle/fairness regressions,
bounded loss/no-route failures and benchmark sample accounting. Existing seeded
join/failure and exhaustive interval tests remain. Observer tests verify no RPCs
or routing mutations occur from global diagnostics.

## Reproducible benchmark

```sh
python3 -m experiments.standalone \
  --sizes 16 32 64 128 --seeds 7 42 20261024 \
  --m 16 --queries 500 --rpc-latency 1 --timeout 5 --period 1 --failures 2 \
  --output reports/standalone-results.json
```

Raw file includes topology/join order, per-query oracle results, hops/latency/RPCs,
separate join and maintenance costs, incorrect-finger history, failure traces and
restoration results. Each case begins with batched joins through one bootstrap.
Neighbors, fingers and lists are verified before healthy measurement.

Ranges span the three seeds, not confidence intervals or averaged percentiles.

| Nodes | Mean hops | Median hops | p95 hops | p99 hops | Join rounds | Failure recovery rounds |
| --- | --- | --- | --- | --- | --- | --- |
| 16 | 2.776–3.000 | 3 | 4–5 | 5 | 28–29 | 16 |
| 32 | 3.364–3.524 | 3 | 5 | 6 | 44–46 | 16–17 |
| 64 | 3.716–3.938 | 4 | 6 | 6 | 74–77 | 11–17 |
| 128 | 4.324–4.494 | 4 | 6 | 7 | 138–142 | 16–17 |

All **6,000 healthy queries were correct**, with zero explicit failures/incorrect
owners. All 12 two-adjacent-node failures converged, as did all 12 restorations;
all 1,200 restoration probes were correct. Of 3,248 recovery probes, 49 failed
explicitly and zero returned an incorrect owner. Other churn schedules may still
produce obsolete ownership through stale live successors; no universal guarantee
is inferred from this workload.

| Nodes | Mean virtual latency / RPCs per lookup | Median latency | p95 latency | p99 latency |
| --- | --- | --- | --- | --- |
| 16 | 7.276–7.936 | 8 | 11–14 | 13–14 |
| 32 | 9.056–9.542 | 8 | 14 | 16–17 |
| 64 | 10.134–10.794 | 11 | 16–17 | 17 |
| 128 | 11.964–12.474 | 11 | 17 | 20 |

These are virtual service units, not milliseconds. With unit cost and no request
loss in this workload, latency equals RPC count, including nested probes.

| Nodes | Join RPCs | Initial maintenance RPCs | Finger repair attempts | Recovery time excluding probes |
| --- | --- | --- | --- | --- |
| 16 | 30 | 5,679–5,902 | 448–464 | 2,977–3,041 |
| 32 | 62 | 18,763–20,101 | 1,408–1,472 | 6,719–7,368 |
| 64 | 126 | 66,004–68,970 | 4,736–4,928 | 10,111–15,565 |
| 128 | 254 | 260,727–268,521 | 17,664–18,176 | 31,567–34,474 |

Each live node attempts one predecessor check, stabilization and finger repair
per round. Initialization costs expose relatively slow batch convergence; no
logarithmic maintenance claim follows from these measurements. Healthy lookups
exclude initialization/maintenance. Recovery records 16 probe queries per state;
raw fields separate probe work, maintenance RPCs and recovery time with probe
work subtracted. No maintenance runs concurrently with those probes.

## Memory observations

Executed separately:

```sh
python3 -m experiments.standalone --sizes 16 32 64 --seeds 20261024 --m 16 \
  --queries 50 --failures 0 --trace-memory \
  --output reports/standalone-memory-results.json
```

| Nodes | Pointer slots/node | Total snapshot JSON bytes | Peak traced workload Python bytes |
| --- | --- | --- | --- |
| 16 | 21 | 21,987 | 269,297 |
| 32 | 21 | 44,163 | 559,092 |
| 64 | 21 | 88,203 | 915,782 |

Pointer slots are 16 fingers, four successors and one predecessor. Snapshots are
exported projections, not resident node internals. Traced allocations include
simulator, history, samples and snapshot creation; they exclude native memory
and process RSS. Byte values are runtime-specific observations, not protocol
reproducibility assertions. This workload differs from the main recovery run.

## Scope, limits and remaining work

The virtual scheduler executes equal-time actions FIFO. RPC requests execute a
handler atomically or are dropped before execution. Unavailable endpoints fail at
normal service cost; dropped requests consume timeout cost. Handlers cannot be
interrupted; overdue events run afterward. This is a deterministic serial model,
not overlapping/reordered in-flight delivery or real network latency.

Stable-ring invariants require convergence. No arbitrary-churn, disjoint-cycle,
partition-healing or Byzantine guarantee is claimed. Exhausting local routes
fails explicitly. Leave is local withdrawal, not atomic graceful neighbor handoff;
the two-node survivor limitation is tested rather than hidden. No storage or
durability is implemented. Section 4 eager joining remains future work.

Next milestone: replay more join/withdrawal/failure schedules and compare
maintenance budgets across seeds; design continuation/state-machine semantics
if overlapping delivery is required. Networking, SWIM and GossipMesh integration
remain separate later work.

The last commit is still `add23bd`. This milestone and preserved earlier local
work remain uncommitted. No new push was requested or performed.
