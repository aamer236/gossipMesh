# GossipMesh: Chord implementation progress report

**Date:** 24 October 2026

**Scope:** Standalone Chord routing module, verification, and research readiness.

**Status:** Implemented and tested for synchronous, in-memory operation. SWIM and
the GossipMesh integration layer remain future work.

## 1. Objective and current outcome

The initial objective was to implement Chord independently before combining it
with failure detection and targeted repair experiments. The repository initially
contained no source code or established tooling. The implementation now provides
a dependency-free Python library, deterministic tests, and a seeded benchmark.

Chord maps a key identifier to the first participating node clockwise at or after
that identifier. Ownership follows `(predecessor, node]`. Each node routes using
its own neighbors and finger table. The transport's endpoint registry delivers
messages; it does not calculate ownership or silently supply membership to routing.

This is a distributed routing algorithm executed inside one Python process with
synchronous simulated communication. It is not yet a deployment across machines.

## 2. Components delivered

| File | Responsibility |
| --- | --- |
| `chord/identifiers.py` | Identifier validation, node/key hashing, modular arithmetic, ring intervals and finger starts |
| `chord/model.py` | Immutable peers, fingers, snapshots, results, events, errors and transport protocol |
| `chord/node.py` | Chord state, lifecycle, routing, stabilization and repair |
| `chord/transport.py` | In-memory RPC delivery, endpoint/link availability and call counters |
| `chord/__init__.py` | Public package exports |
| `tests/test_identifiers.py` | Hashing, arithmetic and exhaustive interval checks |
| `tests/test_routing.py` | Ownership, joins, fingers, neighbors and routing bounds |
| `tests/test_failures.py` | Failures, recovery, local repair and stale/duplicate hints |
| `tests/test_scaling.py` | Seeded routing distributions and production-width identifiers |
| `tests/support.py` | Test orchestration, maintenance driver and independent ownership oracle |
| `experiments/scaling.py` | Reproducible healthy-ring benchmark CLI |
| `README.md`, `context.md` | Usage and persistent engineering handoff |
| `pyproject.toml` | Package metadata and development-tool configuration |

The package targets Python 3.11+ and has no third-party runtime dependencies.
Verification used system Python 3.14.8. Python 3.11 has not been separately tested.
Ruff and mypy are optional development dependencies.

## 3. Algorithms and behavior

### Identifiers and ownership

- Configurable identifier width `m`, from 1 to 256 bits; space size is `2**m`.
- Deterministic SHA-256 hashing with separate domains for node identities and keys.
- Node identifiers are distinct from transport endpoints.
- All four clockwise interval variants support wraparound and explicit equal
  endpoints. Equal endpoints represent one full turn; an open arc excludes its
  endpoint, while inclusive variants cover the whole ring.
- Duplicate node IDs are rejected instead of silently reassigned.

### Joining and distributed lookup

- `create()` immediately establishes a valid singleton ring.
- `join(existing_node)` discovers a successor through distributed lookup.
  The predecessor and higher fingers are initially unknown and converge later.
- `find_successor(id)` and `find_predecessor(id)` resolve ownership and its
  preceding node. `lookup(key)` hashes an application key; `lookup_id(id)` routes
  an existing identifier and returns owner, predecessor, route and hop count.
- Routing chooses a reachable closest preceding finger, falling back to successor
  progress. It checks progress, detects repeated peers and enforces a step limit.
- Failed lookups return explicit errors with partial paths. An unreachable
  immediate successor requires maintenance before safe retry.

### Maintenance and recovery

- `stabilize()` discovers closer successors, refreshes successor lists, and
  notifies its successor.
- `notify(candidate)` accepts an appropriate closer predecessor.
- `check_predecessor()` removes an unreachable predecessor.
- `fix_fingers()` repairs one designated entry, rotating deterministically.
- `maintain()` runs all three periodic operations and reports expected errors.
- Finger starts follow `(node_id + 2**i) mod 2**m`; finger zero mirrors successor.
- Successor lists default to four distinct clockwise peers. Recovery uses locally
  known reachable successors, then local fingers if necessary.
- `leave()` stops participation; neighbors recover through maintenance. It does
  not perform an atomic neighbor splice or transfer application data.

The core does not start background threads or choose real-time maintenance
intervals. A host scheduler or experiment invokes these operations explicitly.

## 4. Failure handling verified

Tests cover unavailable successors, predecessors, fingers, multiple adjacent
failed successors, stale routing entries, exhausted successor lists with a live
finger fallback, endpoint recovery, restored directed links, and leave/rejoin.

Repeated repair requests are safe. Versioned availability hints ignore equal or
older events; hints are reversible and do not imply permanent death.

When no usable route remains, the node fails explicitly. It does not assume it
has become the sole owner of the ring. This includes a survivor of a former
two-node ring with no reachable known peer.

A previously executed 32-node example in an 8-bit space showed:

- Initial convergence in 39 maintenance rounds.
- Lookup of ID 123 from node 0: `0 → 64 → 96 → 112 → 120 → 128`, five hops.
- Crashing node 8 produced an explicit lookup failure before repair.
- Eight maintenance rounds restored consistency among surviving nodes; ID 8
  then belonged to node 16.

These results describe that deterministic scenario, not a bound for every failure
schedule or topology.

## 5. Fresh verification results

The following commands were executed for this report:

```sh
python3 -m unittest discover -v
.venv/bin/ruff format --check chord tests experiments visualizer
.venv/bin/ruff check chord tests experiments visualizer
.venv/bin/mypy
node --check visualizer/static/app.js
git diff --check
```

| Check | Observed result |
| --- | --- |
| Full unit/integration suite | 39 passed in 10.672 seconds; zero failures |
| Test composition | 29 Chord/core benchmark tests; 10 visualizer/HTTP tests |
| Formatting | 18 Python files already formatted |
| Lint | All checks passed |
| Strict type checking | No issues in 8 Python source files |
| JavaScript syntax | Passed |
| Git whitespace check | Passed |

Core tests include exhaustive ownership for all identifiers in small rings,
sequential/batched joins, different join orders, 20 seeded healthy rings,
12 seeded failure subsets, exact predecessor/successor checks, bounded lookup
failure, and a 160-bit identifier-space test without ring enumeration.
Tests do not use arbitrary sleep durations.

Run only the standalone core tests with:

```sh
python3 -m unittest \
  tests.test_identifiers tests.test_routing \
  tests.test_failures tests.test_scaling -v
```

## 6. Fresh benchmark results

Command rerun for this report:

```sh
python3 -m experiments.scaling \
  --sizes 16 32 64 128 --m 16 --queries 1000 --seed 20261024
```

Conditions: reproducibly sampled unique node IDs, batched joins using one
bootstrap, no failures during measurement, and a correctness oracle confirming
neighbors and all fingers before queries begin. Each size uses 1,000 queries.

| Nodes | Mean hops | p95 hops | Correct lookup rate | Convergence rounds | Stabilization calls |
| --- | --- | --- | --- | --- | --- |
| 16 | 2.845 | 4 | 100% | 28 | 448 |
| 32 | 3.344 | 5 | 100% | 46 | 1,472 |
| 64 | 3.927 | 6 | 100% | 76 | 4,864 |
| 128 | 4.430 | 6 | 100% | 142 | 18,176 |

All sizes had zero explicit lookup failures and zero incorrect owners. All
fingers were correct at the convergence gate. Finger-repair attempts during
initialization equal the stabilization-call counts; some attempts can fail while
the ring is still converging.

| Nodes | Lookup-phase route RPCs | Lookup-phase ping RPCs |
| --- | --- | --- |
| 16 | 1,845 | 5,633 |
| 32 | 2,344 | 6,654 |
| 64 | 2,927 | 7,832 |
| 128 | 3,430 | 8,850 |

Hops count overlay edges, including the final edge to the owner. They do not
include all liveness probes. RPC counters are reset after convergence, so these
RPC totals do not include initial join/maintenance traffic.

Interpretation: increasing the ring from 16 to 128 nodes increased mean lookup
hops from 2.845 to 4.430, consistent with finger-table routing avoiding a linear
ring scan in this workload. This single seeded run is not a proof of asymptotic
performance. Initial convergence is comparatively slow with all nodes joining
through the same bootstrap; the measured cost is reported explicitly.

The harness does not yet measure real network latency, statistical confidence
across seeds, concurrent churn, or periodic-versus-targeted repair performance.

## 7. Integration readiness

Available extension points:

- A transport protocol for reachability and well-defined messages.
- Typed join/leave, neighbor-change, repair and lookup events.
- Immutable snapshots for inspecting affected routing references.
- Reversible, optionally versioned availability hints.
- `repair_peer(peer)` for targeted reference repair.
- Individually callable maintenance operations for external scheduling.

Future responsibilities remain separated:

| Layer | Responsibility |
| --- | --- |
| Chord | Key ownership, routing, neighbors, fingers and maintenance |
| SWIM-inspired detector | Probes, suspicion, timeouts, incarnations and membership dissemination |
| GossipMesh integration | Validate membership events, select affected nodes, deduplicate/rate-limit repair and record overhead |

Neither SWIM nor the GossipMesh integration layer has been implemented. Existing
repair hooks establish an interface, not evidence that targeted repair outperforms
the periodic baseline.

## 8. Limits and paper deviations

The implementation follows the 2001 Chord paper's lookup and section 5 lazy
join/stabilization approach. Intentional differences include SHA-256 with hash
domains instead of SHA-1, zero-based fingers, deterministic finger rotation,
bounded iterative lookup, explicit failure behavior and local finger fallback.

Current limitations:

- Communication is synchronous and in-memory; no distributed process/network
  adapter, message-delay model, RPC deadlines, persistence or authentication.
- State mutation requires serialized calls; the library is not thread-safe.
- No storage, replication, key migration, virtual nodes or data durability.
- Stable-ring invariants need convergence. Stale live successors can return an
  obsolete owner during joins/recovery; instantaneous global consistency is not
  promised under churn.
- No general partition/disjoint-cycle healing or arbitrary-churn correctness
  guarantee. Recovery can fail when all locally known routes are unavailable.
- No Byzantine tolerance or production deployment claim.

## 9. Ancillary visualization status

A separate 32-node local visualizer was implemented and tested, using the same
Chord objects. It supports routing-table inspection, crash/restore and periodic
maintenance. It has no dependency in the Chord core and is outside the benchmark
scope of this report. Per the latest direction, further visualization work is
deferred.

## 10. Repository/publication status

The standalone core was previously pushed to
[aamer236/gossipMesh](https://github.com/aamer236/gossipMesh), branch `master`,
commit `add23bd` (initial Chord implementation).

At report generation, subsequent visualization work and documentation/package
updates are local and uncommitted. This report is also a local artifact. No new
push or commit was performed for the report. The preexisting nested `gossipMesh/`
Git repository remains untouched.

## 11. Recommended next milestone

Build a reproducible failure/churn benchmark around a deterministic simulation
schedule before implementing SWIM. Measure lookup success during recovery,
rounds/time to regain consistent routing, stale references, and repair/RPC costs
across multiple seeds and failure patterns. Establish the periodic-maintenance
baseline first, then compare targeted repair under equal maintenance budgets.

If delayed/asynchronous messages are introduced, design explicit continuation or
state-machine semantics rather than inserting sleeps into synchronous RPCs.
SWIM and GossipMesh integration can then be added as separate layers with
versioned membership-event contracts.
