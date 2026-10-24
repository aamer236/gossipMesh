# GossipMesh / standalone Chord engineering handoff

Updated 25 October 2026 (Asia/Kolkata). Root:
`/home/aamer/aamer/projects/gossipMesh`. This document records verified state;
future work is explicitly labeled. Latest milestone: complete and verify local
Chord end to end, without Docker, new networking, SWIM or GossipMesh integration.

## 1. Project overview

The research direction is Chord routing plus a future failure detector and targeted
repair comparison. Current delivered scope is an independently usable Python
Chord routing library, deterministic one-process simulation, tests and reproducible
measurements. Chord maps key IDs to owner nodes; it does not store application
values, transfer files, replicate data or promise durability. No storage layer
was added because it would introduce migration/departure concerns unrelated to
verifying routing. A previously implemented local visualizer remains separate
and unchanged in this milestone; further visualization work is deferred.

## 2. Repository structure

Initial repository had no commits, source, tooling or applicable AGENTS.md. Python
and unittest were selected rather than replacing an existing architecture.
The preexisting untracked `gossipMesh/` contains another empty Git repository;
preserve it. Existing local reports/visualizer/docs were preserved as well.

| Path | Responsibility |
| --- | --- |
| `chord/identifiers.py` | Hashing, ID validation, intervals, modular/finger arithmetic |
| `chord/model.py` | Frozen peer/messages/results/events, errors, Transport protocol |
| `chord/node.py` | Local routing state, join/leave, lookup, maintenance and repair |
| `chord/transport.py` | Endpoint delivery, availability/link controls, RPC counters |
| `chord/__init__.py` | Public exports |
| `simulator/clock.py` | Virtual time, FIFO equal deadlines, cancellation/event budget |
| `simulator/transport.py` | RPC service costs/timeouts, seeded and scripted request loss |
| `simulator/oracle.py` | Independent sorted-ID diagnostic ownership oracle |
| `simulator/ring.py` | Node orchestration, scheduled rounds, failures, diagnostics/history |
| `simulator/__main__.py` | One-command 32-node end-to-end demonstration |
| `simulator/__init__.py` | Simulator exports; core never imports simulator |
| `experiments/scaling.py` | Compatible original healthy scaling CLI |
| `experiments/standalone.py` | Multi-seed healthy/convergence/recovery/memory benchmark |
| `tests/support.py` | Original external maintenance driver and oracle |
| `tests/test_identifiers.py`, `test_routing.py`, `test_failures.py`, `test_scaling.py` | Core verification |
| `tests/test_simulator.py`, `test_standalone_benchmark.py` | Virtual scheduler, faults, exhaustive memberships and measurement accounting |
| `visualizer/`, `tests/test_visualizer*.py` | Preserved prior local dashboard and its 10 tests |
| `reports/chord-paper-audit.md` | Source-to-paper mapping, deviations, reproduced defects |
| `reports/standalone-verification.md` | Current verification and measured-result report |
| `reports/standalone-results.json` | Raw 12-case query samples/phase costs/recovery traces |
| `reports/standalone-memory-results.json` | Separate 3-case allocation/snapshot observations |
| `reports/progress-report.md` | Historical report before the current milestone |
| `README.md`, `pyproject.toml`, `.gitignore` | Usage, package/check configuration and ignored generated files |

Runtime: standard library, target Python 3.11+. Run from root, no installation:
`python3 -m simulator`, `python3 -m unittest discover -q`, or benchmark commands
below. Package metadata includes chord/simulator and preserved visualizer assets.
Package installation itself has not been tested. Optional dev tools live in
`.venv`: Ruff 0.17.2, mypy 2.4.0; uv selected Python 3.13.14 for that environment.
System test/benchmark runtime is Python 3.14.8; Python 3.11 was not separately run.

Publication: root branch `master`, origin `https://github.com/aamer236/gossipMesh.git`.
The initial core was published as `add23bd`. On 25 October the user authorized
committing/pushing all subsequent work, including the simulator, visualizer,
tests, audits, documentation and raw benchmark artifacts. Use `git log` and
`git status` for the current publication state; the earlier report's uncommitted
status describes its original verification date, before this publication request.
Publication recheck: 61 tests passed in 14.922s, 27 Python files formatted,
lint/strict mypy (17 source files)/JS syntax passed. Staged whitespace review
found two Markdown trailing-space line breaks in the historical progress report;
changed them to paragraph breaks before committing.

## 3. Paper and algorithm decisions

Original reference actually read: `/home/aamer/Downloads/chord_sigcomm.pdf`, Stoica
et al., SIGCOMM 2001. See reports/chord-paper-audit.md for full mapping.
Implemented Section 4.3/Figure 4 lookup and **Section 5.1/Figure 7 lazy join and
stabilization**, plus Section 5.2 successor lists. Section 4.4/Figure 6 eager
init_finger_table/update_others is an alternative strategy, not implemented here.
Do not claim every part of the paper or its complete failure-resilience theorems.

Intentional differences: SHA-256 with node/key domains instead of SHA-1; zero-based
fingers; deterministic rotating repair rather than random index selection;
iterative bounded lookup/progress checks; conservative failure when the immediate
successor is unavailable; local finger fallback when successor-list alternatives
are exhausted; optional events/reversible/versioned reachability hints/repair hook.

Invariants:

1. Integer IDs in `[0,2**m)`, integer width 1..256. No production-space enumeration.
2. Circular ordering uses modular distance. Equal endpoints represent a full
   turn: `(a,a)` excludes a; inclusive variants include the whole ring.
3. Once converged, successor/predecessor are the next/previous participating live
   node; ownership is `(predecessor,node]`. Global partitions cannot be certified
   from local reachability alone.
4. Finger i starts at `(node+2**i)%2**m` and converges to its successor. Finger zero
   always mirrors primary successor. Unknown/stale higher fingers are allowed
   during transitions; a correct successor backbone can route with stale fingers.
5. Successor lists are distinct clockwise peers, capacity bounded, excluding self
   in a multi-node ring. A newly notified creator may temporarily retain its old
   self-successor; lookup then fails until stabilization adopts the neighbor.
6. Routing steps progress strictly in `(current,target)`; terminal resolution uses
   `(current,successor]`. Work is bounded; transport adapters must bound RPCs too.
7. State is private; snapshots/messages are frozen with tuple tables/lists. Updates
   use validated protocol operations. IDs and endpoints are distinct bindings.
8. Registry knowledge delivers messages only. Oracles may observe and stop an
   experiment, never choose routes or inject predecessor/successor/finger state.

Create immediately forms a valid singleton. Lazy join learns one successor,
sets finger zero and leaves predecessor/higher fingers unknown. Notify/stabilize
establish neighbors; rotating repair fills fingers. Churn can leave stale live
successors and temporarily obsolete owners. This is a transitional limitation
counted separately from explicit failures, not a stable-ring ownership guarantee.

Verified/fixed defects in this milestone: old lifecycle hints blocked a fresh
explicit rejoin; failed finger lookup pinned the rotating cursor. Tests reproduced
both failures before fixes. New join clears old hints before bootstrap; create/join
reset the cursor; validated repair advances it even when lookup fails while
retaining the old entry and emitting no fake success event.

## 4. Architecture and interfaces

`IdentifierSpace(m=160)` supports validate/add/distance/finger_start,
`node_id(str|bytes)`, `key_id(str|bytes)`, and
`contains(value,start,end,*,left_closed=False,right_closed=False)`.
Hash domains: `b"chord:node\0"` and `b"chord:key\0"`, UTF-8 or raw bytes,
SHA-256 big-endian integer modulo 2**m. Small-space collisions can happen;
duplicate node IDs are rejected. Hash stable identities independently of addresses.

`Peer(id,endpoint)` is frozen. `ChordNode(peer,space,transport,*,
successor_list_size=4,max_hops=512,on_event=None)` owns local state. Public APIs:

- create(), join(existing_node), leave(). Leave is clean local withdrawal, not
  atomic neighbor handoff or key transfer; reachable alternatives are needed.
- lookup(key) hashes input; lookup_id(id,*,max_hops=None) returns LookupResult with
  owner, predecessor, path and hops. Hops count overlay edges including final
  owner edge, not liveness probes. Singleton lookup has zero hops.
- find_successor(id), find_predecessor(id), closest_preceding_finger(id).
- stabilize(), notify(candidate), check_predecessor(), fix_fingers(index=None).
  Repair rotates one entry per call, including unsuccessful attempts.
- maintain() returns MaintenanceResult.errors with expected operation failures;
  programming errors propagate. These methods are individually callable.
- set_peer_available(peer,available,*,version=None): reversible local hint, not
  transport availability/death. Equal/older versions ignored within a lifecycle;
  unversioned hints are authoritative overrides. New join starts fresh hints.
- repair_peer(peer): idempotent where practical, neighbors plus affected fingers;
  does not mark peer dead. Optional typed Event callbacks observe state changes,
  lookup outcomes, availability hints, repairs and lifecycle. Callbacks must not
  raise/reenter; no history accumulates without an observer.
- Read-only peer, space, state; route_step(id) is the routing message handler.

Errors: LookupFailed(reason,partial_path), PeerUnavailable, InvalidState; invalid
caller IDs/parameters raise ValueError/TypeError. No-route never silently becomes
whole-ring ownership. A survivor of a former two-node ring needs known-peer return
or explicitly supplied bootstrap, including after a neighbor's local leave.

Transport interface: ping(source,target)->bool, read_state(...)->RoutingState,
route(source,target,id)->RouteStep, notify(source,target). InMemoryTransport
registers endpoints and offers set_available(peer,bool), directed set_link and
RPC call counters. Crash preserves active node state; hosts skip its scheduling.

`ChordSimulation(m=16,seed=0,latency=0,timeout=5,drop_rate=0,
successor_list_size=4,max_hops=512)` is external orchestration. APIs: add_node(id,
bootstrap=None), live_nodes(), oracle(), diagnostics(), maintenance_round(),
run_rounds(count,period=1), converge(max_rounds=1000,period=1), crash(id), recover(id),
leave(id), rejoin(id,bootstrap), trace(). Active crashed nodes use recover; withdrawn
nodes use rejoin. Explicit rejoin makes the source available before bootstrap.
Failed joins do not leave a new unusable registration. Diagnostics verify exact
fingers, neighbors and ordered successor lists against independent sorted IDs.

Clock: call_at/call_later callbacks, cancel handles, run_until(until,max_events),
consume(cost). Equal deadlines FIFO; callbacks serialized/atomic. Request-specific
and seeded random drops, per-link latency and directed links live in transport.
Each RPC consumes configured service cost, dropped requests timeout cost;
unavailable endpoints fail at normal service cost. Nested probes add cost.
Scheduled callbacks cannot interrupt handlers; overdue events run afterward in
original deadline order. This is NOT overlapping/reordered packet simulation.
Virtual latency is in service units, not milliseconds/real network latency.

Benchmark phases separately measure join, initial maintenance, healthy lookups,
adjacent failure with probes, and saved-state restoration. Percentiles nearest-rank.
Hops cover correct lookups; all-attempt latency/RPCs and successful latency are
separate. Empty distributions are null. Recovery includes probes and separately
reports time with probe service work subtracted. Memory scopes are explicit:
exported snapshots vs optional traced workload allocations, never process RSS or
an assertion about only persistent node heap. Tracing is cleaned up on errors.

## 5. Implementation status

Current milestone complete: paper audit, minimal defect fixes, virtual simulation,
exhaustive oracle verification, reproducible multi-seed metrics/raw results, docs.
Latest full suite: 61 passing tests. No known failing tests.

Not implemented/known limits: Section 4 eager join, atomic graceful handoff,
application storage/replication/migration, persistence, actual network RPC,
concurrent handlers/in-flight delivery, disjoint-cycle/partition healing, Byzantine
resistance, arbitrary-churn guarantees or a general proof for the failure extension.
Successor lists default to 4, not a theorem-derived log N parameter. Old visualizer
is retained but is not the headless simulator/measurement architecture.

## 6. Testing and verification

Commands actually run after final code edits:

```sh
python3 -m unittest discover -q
# 61 passed in 12.567s; zero failures.
.venv/bin/ruff format chord simulator tests experiments visualizer
# 2 files reformatted, 25 unchanged in the final formatting invocation.
.venv/bin/ruff format --check chord simulator tests experiments visualizer
# 27 files already formatted in final verification.
.venv/bin/ruff check chord simulator tests experiments visualizer
# All checks passed.
.venv/bin/mypy
# Strict success: 17 source files (core, simulator, experiments, preserved UI).
node --check visualizer/static/app.js
python3 -m compileall -q chord simulator experiments tests visualizer
git diff --check
# All exit 0.
python3 -m simulator
# 32 nodes, 39 join rounds; lookup 123 path [0,64,96,112,120,128], 5 hops;
# node 8 crash gives explicit failure, 8 recovery rounds → owner 16;
# restore: 9 rounds → owner 8; final 32 online, all diagnostic errors zero.
```

Baseline 39 tests: 10.521s. Then two deliberately failing regressions (one failure,
one error) passed after fixes; intermediate suites 56/59/60 passed before final 61.
Strict check expansion found one missing list annotation in legacy scaling; fixed.
An additional failing simulator regression exposed a cancelled-event budget edge:
cancelled callbacks were considered after the active-callback budget check. Fixed
by discarding cancelled handles before checking the budget; pending live events
remain queued on exhaustion. The raw benchmark uses no cancelled events and is
unaffected. The README scheduled-failure example and both raw JSON files were
also checked successfully after final edits.
Historical environment/tool download and socket errors are documented in older
reports; none is an unresolved test failure. No sockets were opened this milestone.

Coverage includes every interval variant/value/endpoints at m=4; all 255 nonempty
membership subsets at m=3 with every source/key checked; 24 sequential permutations
of four IDs; prior batched/different-order joins and seeded failure subsets;
converged fingers/neighbors; stale live fingers with correct backbone; failure
hints/events/repair duplication; unknown peers/IDs, bounded/no-route failures;
virtual clock FIFO/cancellation/budget; seeded drops, no-delivery on dropped notify,
directed links, timing scaling, scheduled failures/recovery, withdrawn offline
rejoin, no oracle mutations/RPCs, measurement accounting/replay and tracer cleanup.
Preserved visualizer/HTTP tests remain in the suite; tests need no sleeps/sockets.

Raw benchmark actually executed:

```sh
python3 -m experiments.standalone --sizes 16 32 64 128 --seeds 7 42 20261024 \
  --m 16 --queries 500 --rpc-latency 1 --timeout 5 --period 1 --failures 2 \
  --output reports/standalone-results.json
python3 -m experiments.standalone --sizes 16 32 64 --seeds 20261024 --m 16 \
  --queries 50 --failures 0 --trace-memory \
  --output reports/standalone-memory-results.json
```

12 primary cases, 6000/6000 correct healthy queries, 0 failed/incorrect owners.
All 12 adjacent-pair failure scenarios and restorations converged. Recovery probes:
3248 attempts, 49 explicit failures, 0 incorrect owners. Restoration: 1200 correct
probes. Mean-hop ranges across three seeds: 16 nodes 2.776–3.000; 32 3.364–3.524;
64 3.716–3.938; 128 4.324–4.494. p99 hops 5/6/6/7. Failure recovery rounds
16 / 16–17 / 11–17 / 16–17. Full median/tail virtual latency, RPCs, join/convergence
costs and incorrect-finger traces are in raw JSON and verification report.

All nodes batch-join one bootstrap; initial convergence is slow (28–29,44–46,
74–77,138–142 rounds). No asymptotic/latency/churn guarantee is inferred.
Healthy query metrics exclude initialization/maintenance. Recovery probes consume
serial work, explicitly separated in time/RPC fields. Approx. 3.9MB primary raw
JSON and 100KB memory JSON are intentional research artifacts. Memory run is a
separate 50-query/no-failure workload: 21 pointer slots/node; peak traced Python
allocations 269297/559092/915782 bytes at 16/32/64. Scope includes simulation,
history and samples; runtime-specific, not process RSS. Historical original
single-seed 1000-query results remain in reports/progress-report.md.

## 7. Design decisions

Preserved Python/unittest/no runtime dependencies, immutable messages, synchronous
core and Section 5 lazy maintenance. Membership/maintenance remain in ChordNode
because they own the same state. Transport does not use its registry as a lookup
oracle. New simulator wraps the existing transport/core instead of replacing
working code. Original scaling command remains compatible.

The important extension decision is explicit virtual serial service time rather
than an invasive asynchronous rewrite. This satisfies deterministic local event
scheduling/delay/loss experiments but not concurrent packet realism. Extending
to in-flight messages requires continuation/state-machine design. The only core
behavior changes fix reproduced lifecycle/cursor defects. Availability hint
ordering is lifecycle-scoped; external incarnation reconciliation remains caller
policy. Storage intentionally deferred. No earlier architectural decision was
silently reversed. Existing visualization remains independent and untouched.

## 8. Future GossipMesh integration plan

Not implemented in this milestone. Chord continues to own key ownership/routing,
neighbors/fingers and stabilization. A future SWIM-like detector will own probes,
suspicion, deadlines, incarnations and membership dissemination. Future GossipMesh
policy will validate/version hints, choose affected nodes, deduplicate/rate-limit
repairs and compare overhead against periodic maintenance with equal budgets.
Use existing on_event/state inspection/set_peer_available/repair_peer interfaces;
no concrete SWIM imports. Suspect must not automatically mean permanently dead.

## 9. Next steps

1. Most useful next task: add replayable mixed join/withdrawal/failure workload
   scenarios and systematic maintenance-budget sweeps, keeping oracle errors,
   explicit failures and excluded assumptions separate across seeds.
2. If overlapping messages are needed, design asynchronous continuations/state
   machines, then test delayed/reordered replies rather than adding sleeps.
3. Consider atomic graceful-leave handoff or eager joining only as separately
   specified/tested strategies. Storage needs independent migration/durability
   semantics. None is required for the selected local routing scope.
4. Only after those baselines, specify detector incarnation events and future
   SWIM/GossipMesh comparisons. No current blocker for the delivered milestone.

## 10. Agent handoff instructions

Read context, README, reports/chord-paper-audit.md and standalone-verification.md,
then chord/model.py/node.py/transport.py, simulator and benchmark source/tests.
Establish state with git status --short, git log -1 --oneline, python3 --version,
full unittest, Ruff format/check and mypy commands above. Run python3 -m simulator
for a short headless smoke test. Raw JSON schemas/configuration are in each file.
Do not confuse raw metric observations with mathematical guarantees.

Preserve current user/uncommitted work and nested Git directory. Do not silently
change hashing/domains, full-turn intervals, ownership, finger starts/zero alignment,
bounded lookup, immutable inspection, cursor fairness, explicit errors, local-only
routing knowledge or version/lifecycle semantics. Do not replace protocol repair
with global oracle assignments. Additions must remain independent of networking,
SWIM and integration policy unless a later user explicitly changes scope. Update
this document at each milestone; never claim unexecuted verification.
