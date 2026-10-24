# GossipMesh engineering handoff

Last updated: 2026-10-24 (Asia/Kolkata). Facts below are verified unless marked
as planned. Root: `/home/aamer/aamer/projects/gossipMesh`.

## 1. Project overview

GossipMesh will study Chord routing plus SWIM-inspired failure detection and
targeted repair. Current scope is a standalone, deterministic Chord key-to-node
lookup library. Chord owns hashing, circular routing, neighbors, fingers, and
maintenance. No SWIM, storage, file transfer, network daemon, or durability claim.

## 2. Repository structure

Initial assessment: root Git repository had no commits, source, tests, manifests,
or documentation, and no established language. No applicable AGENTS.md was found.
The preexisting untracked `gossipMesh/` contains a second empty Git repository
(no commits); preserved untouched. The implementation lives in the root repository.
Publication remote: `https://github.com/aamer236/gossipMesh.git`, branch `master`.
Use `git status` and `git log` for current commit/publication state.

- `chord/identifiers.py`: hashing, validation, arithmetic and intervals.
- `chord/model.py`: immutable peers/messages/results/events, errors, Transport.
- `chord/node.py`: local state, lifecycle, routing, maintenance and repair.
- `chord/transport.py`: in-memory message delivery, failures/links, RPC counts.
- `chord/__init__.py`: public exports.
- `tests/support.py`: external test oracle and deterministic maintenance driver.
- `tests/test_identifiers.py`, `test_routing.py`, `test_failures.py`,
  `test_scaling.py`: identifier, convergence, failure and scaling checks.
- `experiments/scaling.py`: seeded scaling CLI and external ownership oracle.
- `tests/__init__.py`, `experiments/__init__.py`: package/discovery entry points.
- `README.md`: example, commands, public behavior and limitations.
- `pyproject.toml`: Python 3.11+ package, optional dev tools, Ruff/mypy config.
- `.gitignore`: Python, virtual environment, and tool artifacts.

Runtime has no dependencies beyond Python. Run from root:
`python3 -m unittest discover -v` and `python3 -m experiments.scaling`.
No installation is needed to run locally. Package install: `pip install .`;
dev setup: `pip install -e '.[dev]'`. Packaging itself was not exercised.

## 3. Paper and algorithm decisions

Reference actually read: `/home/aamer/Downloads/chord_sigcomm.pdf`, Stoica et al.,
SIGCOMM 2001, sections 4–5, especially lookup Figure 4, lazy join/stabilization
Figure 7, and successor lists in section 5.2.
Implemented consistent hashing, successor/predecessor lookup, closest preceding
fingers, create, section 5 lazy join, stabilize, notify, check_predecessor,
rotating finger repair, and successor lists. Section 4 eager update_others is
intentionally replaced by the paper's section 5 approach for concurrent joins.

Differences/extensions: SHA-256 with node/key domains replaces SHA-1; zero-based
fingers; deterministic rotating repair instead of random index selection;
bounded iterative routing with explicit errors; local finger fallback after
successor-list exhaustion; reversible reachability hints, events and targeted
repair. Leave deactivates rather than atomically splicing neighbors.

Correctness invariants:

1. IDs are integers in `[0, 2**m)`, integer `1 <= m <= 256`.
2. Ordering is clockwise/modular. Equal-endpoint arcs mean one full turn:
   `(a,a)` excludes a; `(a,a]`, `[a,a)`, `[a,a]` cover the whole ring.
3. After convergence, successor/predecessor are next/previous participating
   reachable nodes; ownership is `(predecessor, node]`.
4. Finger i has designated start `(node_id + 2**i) % 2**m`; its converged peer
   is successor(start). Finger zero always mirrors the primary successor.
5. Successor lists contain distinct clockwise peers, bounded by capacity, excluding
   self except for a known singleton. A newly notified creator temporarily retains
   its self-successor until stabilization adopts a neighbor; lookups fail then.
6. Forward steps lie strictly in `(current,target)`; termination uses
   `(current,successor]`. Lookups finish or fail within the route-step budget;
   RPC adapters must independently bound their calls. Unreachable peers never
   trigger unbounded loops. Losing all known peers never invents a singleton.
7. State is private, snapshots/messages frozen, fingers/lists tuples; updates use
   validated operations. Conflicting ID/endpoint bindings are rejected.
8. Registry knowledge is for endpoint delivery only. Sorting/membership ownership
   oracles live only in tests/experiments, never in routing.

Create is immediately a valid singleton. Join learns only a successor, sets
finger zero, leaves predecessor None and higher fingers None. Notification makes
the node discoverable; stabilization establishes neighbors and lists; finger
repair restores routing efficiency. Stale/missing neighbors and fingers during
churn are transitional. A live but stale successor can return an obsolete owner
while joining/recovering; local state cannot certify global ownership in arbitrary
partitions. Stable-ring guarantees are conditional, not atomic join guarantees.

## 4. Architecture and interfaces

`IdentifierSpace(m=160)` provides `validate(id)`, `add(id,offset)`,
`distance(start,end)`, `finger_start(id,index)`, `node_id(str|bytes)`,
`key_id(str|bytes)`, `contains(value,start,end,*,left_closed=False,
right_closed=False)`. Hash input is UTF-8 or bytes, prefixed by
`b"chord:node\0"` or `b"chord:key\0"`, SHA-256 interpreted big-endian then
modulo 2**m. No enumeration of the identifier space. `Peer(id,endpoint)` is frozen;
ID and endpoint are distinct. Small-space node collisions are rejected rather
than silently reassigned. Key IDs and node IDs share arithmetic, with distinct
hash entry points. Hash stable identities independently of endpoint addresses.

`ChordNode(peer,space,transport,*,successor_list_size=4,max_hops=512,on_event=None)`:

- `create()`, `join(existing_node: Peer)`, `leave()`.
- `lookup(key: str|bytes) -> LookupResult`; hashes application key.
- `lookup_id(id,*,max_hops=None) -> LookupResult`; routes an existing key ID.
- `find_successor(id) -> Peer`, `find_predecessor(id) -> Peer`,
  `closest_preceding_finger(id) -> Peer` (self means no useful preceding finger).
- `stabilize()`, `notify(candidate)`, `check_predecessor()`,
  `fix_fingers(index=None)`; omitted index rotates, repairs one designated entry.
- `maintain() -> MaintenanceResult`: predecessor check, stabilization, one finger
  repair. Expected ChordError failures become operation-labeled error strings;
  programming errors propagate. Individual operations may raise.
- `set_peer_available(peer,available,*,version=None)`: local reversible exclusion
  hint, not transport state/death declaration. Versioned hints ignore equal/older
  versions per Peer; unversioned calls are authoritative administrative overrides.
  True permits probes again without installing routes.
- `repair_peer(peer) -> MaintenanceResult`: recheck neighbors, repair fingers
  referring to peer. Safe to repeat; does not mark the peer permanently dead.
- Read-only `peer`, `space`, `state: RoutingState`; `route_step(id)` is an RPC
  handler. State has peer, predecessor, ordered successors, fingers and active;
  `successor` is primary entry or None. No arbitrary public pointer setters.

LookupResult contains owner, predecessor, path, computed hops. Hops count overlay
edges including final owner edge (zero for singleton), not probes. LookupFailed
has reason/partial path. InvalidState handles lifecycle/binding conflicts;
PeerUnavailable handles reachability. Invalid caller values raise ValueError or
TypeError. Availability is external; active means joined and not left.

Transport protocol: `ping(source,target)->bool`,
`read_state(source,target)->RoutingState`, `route(source,target,id)->RouteStep`,
`notify(source,target)`. Adapters must bound calls, validate/authenticate peer
bindings and space compatibility, translate unreachable requests to
PeerUnavailable. Remote node operations use messages rather than private fields.
InMemoryTransport registers endpoints and offers `set_available(peer,bool)`
(crash/recover with state retained), `set_link(source,target,bool)` (directed),
and `calls: Counter` for ping/read_state/route/notify RPC counts.
No storage was added because none existed. Future storage must separately own
migration, replication and durability; returning the owner does not store data.

## 5. Implementation status

Completed milestones: assessment/context; identifiers (4 tests); routing and
convergence (13); resilience and hooks (24); scaling/160-bit check (26); final
boundary, notification, and seeded failure tests (29). Standalone synchronous
scope complete. No known failing tests.

Known limitations/technical debt:

- Synchronous serialized calls, not thread-safe; no actual network adapter,
  scheduler, message-delay model, persistence, process restart, or authentication.
- No partition/disjoint-cycle healing, Byzantine tolerance, arbitrary-churn
  correctness, or general convergence proof for the failure extensions.
- An isolated survivor fails closed even if globally alone. It needs a returning
  known peer or external bootstrap. Explicit leave then join(bootstrap) can
  rebootstrap when an external mechanism supplies a reachable peer.
- Saved-state recovery is tested with a surviving connected ring. Hosts must stop
  scheduling offline nodes; transport availability does not suspend Python calls.
- Versioned hints need monotonic per-peer versions including incarnations in the
  future integration. Unversioned overrides deliberately bypass version ordering.
- Optional synchronous event callbacks must not raise or reenter the node.
  No event history accumulates without an observer. Full snapshots and probes
  prioritize clarity over optimized message/CPU costs.

## 6. Testing and verification

Actual system Python: 3.14.8. uv created `.venv` with Python 3.13.14. Target is
3.11+; 3.11 was not separately tested. Dev tools: Ruff 0.17.2 and mypy 2.4.0.
Actual latest commands/results:

```sh
python3 -m unittest discover -v
# 29 tests passed in 6.439s, exit 0, no failures.
.venv/bin/ruff format chord tests experiments
# Formatting applied; final run: 13 files left unchanged.
.venv/bin/ruff check --fix chord tests experiments
# Three import-order findings corrected; zero remaining.
.venv/bin/ruff format --check chord tests experiments
# 13 files already formatted, exit 0.
.venv/bin/ruff check chord tests experiments
# All checks passed, exit 0.
.venv/bin/mypy
# Strict: no issues found in 5 source files, exit 0.
python3 -m compileall -q chord tests experiments
# Exit 0.
git diff --check
# Exit 0. At initial verification files were untracked; Ruff checked source directly.
python3 -m experiments.scaling --sizes 16 64 128 --queries 1000
# Exit 0; observed results below.
```

Baseline test discovery: zero tests, exit 5. Initial Ruff/mypy commands failed
because tools were absent. Initial uv install failed sandbox DNS; approved retry
installed them. Initial lint found three import-order issues, fixed with --fix.
No outstanding tooling failures; no failing test reproduction is currently needed.
Final smoke check also executed the README example successfully, parsed
pyproject.toml with tomllib, confirmed all ten handoff sections, and verified
the nested repository still has clean Git status. Root files were untracked at
the end of initial implementation; the user subsequently requested GitHub publication.

Coverage: exhaustive all four interval variants/values/endpoints at m=4;
hash determinism/domain/truncation, arithmetic and starts; exhaustive every key
from every node in small rings, exact owner/predecessor; singleton/two-node/empty;
sequential, batched and varied-order joins; 20 seeded randomized healthy rings;
closest preceding fingers; bounded budget and invalid-progress rejection;
immutable snapshots, binding checks, stale notifications; successor/predecessor/
finger failures; three adjacent failed successors; list exhaustion with finger
fallback; no usable routes, isolation; recovery and leave/rejoin; restored directed
links; duplicate/stale-version hints and repairs; 12 seeded failure subsets
(1–3 of 9 nodes) with exhaustive ownership and recovery; scaling at 16/32/64;
160-bit IDs without ring enumeration. No tests use sleeps.

Healthy experiment: seed 20261024, m=16, 1000 lookups per size:

| Nodes | Mean hops | p95 | Success | Fingers correct | Ticks to converge | Stabilizations |
| --- | --- | --- | --- | --- | --- | --- |
| 16 | 2.845 | 4 | 100% | 100% | 28 | 448 |
| 64 | 3.927 | 6 | 100% | 100% | 76 | 4864 |
| 128 | 4.430 | 6 | 100% | 100% | 142 | 18176 |

Zero failed lookups/incorrect owners. Route RPCs: 1845/2927/3430, ping RPCs:
5633/7832/8850 respectively. Finger repair attempts equal stabilization counts;
attempts during convergence can fail. External oracle verifies all neighbors and
fingers before querying. These are distribution-level healthy-ring observations,
not a latency model or proof under churn. Initial batch joins all use one bootstrap
and settle relatively slowly; no logarithmic maintenance claim is made.

## 7. Design decisions

Python + unittest chosen because no project tooling existed; no prior code was
replaced. Standard library minimizes setup. Synchronous RPC-like messages with
explicit maintenance ticks preserve reproducibility and permit later scheduling
without threads/sleeps. No speculative SWIM/storage was introduced.
Membership and maintenance share ChordNode state; separate modules per operation
would add coupling without benefit. Immutable messages separate core and transport.
Registry uniqueness checks at registration are allowed; registry-based routing is
not. External test/experiment oracles may inspect membership.
Preserve lazy convergence, conservative no-route errors, bounded work, validated
updates and hashing compatibility. No architectural decision was reversed during
implementation. Dev tools are optional; strict mypy covers the library, runtime
unit tests intentionally include invalid caller types/values.

## 8. GossipMesh integration plan (proposed)

Supply `on_event(Event)` for JOINED/LEFT, SUCCESSOR_CHANGED/PREDECESSOR_CHANGED
(with previous/new peer), PEER_UNAVAILABLE/PEER_AVAILABLE (explicit local hints),
FINGER_REPAIRED (index), REPAIR_REQUESTED, LOOKUP_SUCCEEDED and LOOKUP_FAILED
(reason). Use frozen snapshots to inspect affected references. Translate validated
membership events to set_peer_available and repair_peer; no pointer setters needed.
SWIM will own direct/indirect probes, suspicion/death states, deadlines,
incarnations, and dissemination. Chord retains ownership/routing/maintenance.
GossipMesh will own event validation/versioning, choosing affected nodes,
deduplication, rate limits, repair overhead and baseline comparisons. Suspicion
must not silently imply permanent death. Keep periodic maintenance as baseline.
Before integration, add a latency-aware deterministic scheduler/churn workloads,
define membership/incarnation semantics, and test reordered events/equal budgets.
Chord must remain usable and testable with no concrete SWIM import.

## 9. Next steps

1. Most useful next task: deterministic discrete-event simulation around the
   Transport/maintain interfaces, delayed messages, seeded churn/partitions and
   explicit repair/RPC metrics. Async delivery requires continuation/state-machine
   design; do not add arbitrary sleeps to this synchronous core.
2. Establish periodic-repair baseline and investigate known partition/disjoint
   cycle limits before claiming stronger guarantees.
3. Specify versioned membership/incarnation events; implement SWIM separately.
4. Implement GossipMesh translation and compare targeted/periodic repair with
   equal budgets. Networking, storage and virtual nodes are independent options.
No blocker for the completed standalone synchronous scope.

## 10. Agent handoff instructions

Read context.md, README.md, chord/model.py, chord/node.py, chord/transport.py,
then tests/support.py and tests. Establish current state with:

```sh
git status --short
python3 --version
python3 -m unittest discover -v
.venv/bin/ruff check chord tests experiments
.venv/bin/mypy
```

New library/test/docs/experiment files are safe to evolve within task scope;
preserve user changes and the preexisting nested `gossipMesh/` Git repository.
Never break domain hashing, clockwise/equal-endpoint
semantics, ownership, designated starts, finger-zero/successor alignment,
encapsulation, bounded routing, explicit errors, local route knowledge,
reversible/versioned hints, or standalone operation. Do not replace routing with
an oracle or broaden guarantees silently. Re-run relevant checks and update this
file at every milestone.
