# GossipMesh: standalone Chord

A dependency-free Python 3.11+ implementation of decentralized Chord key-to-node
lookup. Chord has no dependency on SWIM, an integration layer, or a storage system.
The transport included here delivers synchronous simulated messages; it is not a
network server. Read [context.md](context.md) for the engineering handoff.

## Run

From the repository root:

```sh
python3 -m unittest discover -v
python3 -m experiments.scaling --sizes 16 64 128 --queries 1000
```

For the complete deterministic, one-process milestone:

```sh
python3 -m simulator
python3 -m experiments.standalone --sizes 16 32 64 128 --seeds 7 42 20261024 \
  --m 16 --queries 500 --rpc-latency 1 --timeout 5 --period 1 --failures 2 \
  --output reports/standalone-results.json
```

The demo joins 32 nodes, routes a key, crashes node 8, observes an explicit failure,
repairs surviving routes and restores the endpoint. It uses no sockets, threads
or sleeps. See the [paper audit](reports/chord-paper-audit.md) and
[verification report](reports/standalone-verification.md) for algorithm scope,
results and limits. Section 5 lazy maintenance is implemented; Section 4 eager
`init_finger_table`/`update_others` remains a separate future strategy.

## Deterministic simulation and measurement

`simulator.ChordSimulation(m=16, seed=0, latency=0, timeout=5, drop_rate=0)` owns
external orchestration, virtual time and delivery. APIs: `add_node(id, bootstrap=None)`,
`run_rounds(count, period=1)`, `converge(max_rounds=1000, period=1)`, `crash(id)`,
`recover(id)`, `leave(id)`, `rejoin(id, bootstrap)`, `diagnostics()` and `trace()`.

```python
from simulator import ChordSimulation

simulation = ChordSimulation(m=8, seed=7, latency=1)
for identifier in range(0, 256, 8):
    simulation.add_node(identifier)
simulation.converge()
simulation.clock.call_later(1, lambda: simulation.crash(8))
simulation.run_rounds(1)
simulation.converge()
assert simulation.nodes[0].find_successor(8).id == 16
```

`clock.call_at(when, callback)` and `call_later(delay, callback)` order scheduled
actions by virtual deadline, breaking ties FIFO. Cancellation and finite event
budgets are supported. Transport controls: `set_link(source,target,bool)`,
`set_latency(source,target,cost)`, `drop_next(source,target,count=1)` and
`set_drop_rate(probability)`. Drops use a private seeded generator; a dropped
request never executes its handler.

The synchronous core stays serialized: RPCs consume configured virtual service
costs and dropped requests consume timeout costs. Nested probes add their cost.
Unavailable endpoints fail at the normal service cost. Scheduled callbacks never
interrupt a handler; overdue callbacks run afterward in deadline order. These
are **virtual service units**, not milliseconds or a model of overlapping,
reordered packet delivery. Maintenance does not run concurrently during a lookup.

The richer benchmark records all query samples, explicit failures separately from
incorrect owners, mean/median/p95/p99 hops and virtual latency, RPCs per attempt,
separate join/convergence costs, incorrect-finger traces, maintenance overhead,
adjacent-failure recovery and restoration. Percentiles use nearest rank. Hops
cover correct lookups; latency/RPC distributions cover all attempts, with successful
latency also separate. Empty distributions are null, not fabricated zeros.

Healthy measurements exclude initialization/maintenance. Recovery includes 16
probes per round (or fewer when queries < 16); total time, probe cost, maintenance
RPCs and recovery time excluding probe work are reported separately. Global
diagnostics only observe membership; they never supply routes or modify state.
Raw baseline: `reports/standalone-results.json`, 12 cases and 6,000 healthy queries.

To measure Python allocations separately:

```sh
python3 -m experiments.standalone --sizes 16 32 64 --seeds 20261024 --m 16 \
  --queries 50 --failures 0 --trace-memory \
  --output reports/standalone-memory-results.json
```

Memory fields distinguish exported routing-snapshot sizes/pointer slots from
tracemalloc allocations for the whole case (including diagnostics and samples).
They do not measure process RSS or only the persistent Chord node heap. Bytes
vary by runtime; memory observations are not protocol determinism assertions.
No storage layer is included in this routing milestone.

## Visual simulator

```sh
python3 -m visualizer --port 8765
```

Open **http://127.0.0.1:8765**. The local dashboard runs 32 actual Chord nodes
at IDs 0, 8, …, 248 in an 8-bit identifier space. It starts with converged tables.

- Hover over a node to preview its eight finger entries, predecessor, successor,
  and successor list. Click to pin its inspector and finger connections.
- Right-click a node to crash its simulated endpoint. An inspector button offers
  the same action for touch/keyboard users. Offline nodes remain visible with
  frozen state; live nodes retain stale pointers until maintenance repairs them.
- Every second, each live node runs `check_predecessor`, `stabilize`, and one
  rotating `fix_fingers` operation through `maintain()`. The activity feed shows
  neighbor changes, finger updates, and per-tick errors.
- Pause/Resume, step one tick while paused, change the interval (0.5/1/2 seconds),
  restore an endpoint, or reset all 32 nodes. Reset resumes the default schedule.
- Hover/focus a finger row to highlight its edge. Toggle fingers/successor edges
  independently. On narrow screens the ring scrolls horizontally rather than
  shrinking nodes into overlapping targets.

This is a single shared, in-memory simulation across tabs. The single-threaded
HTTP server serializes actions and maintenance and keeps running when the browser
is closed. Restarting/resetting loses session state. Bind address is loopback only;
this is a local demonstration server, not a hosted multi-user application.
The global observer computes correctness/stale-entry diagnostics but never
injects routes. Crash does not broadcast failure or invoke targeted repair.
The Chord core is unchanged. Large failures can exhaust local routes; the UI
reports incomplete recovery rather than replacing tables from global membership.

No installation is required. To install as a package, use `pip install .`.
Development tools are optional: `pip install -e '.[dev]'`.

```sh
.venv/bin/ruff format --check chord simulator tests experiments visualizer
.venv/bin/ruff check chord simulator tests experiments visualizer
.venv/bin/mypy
```

## Example

```python
from chord import ChordNode, IdentifierSpace, InMemoryTransport, Peer

space = IdentifierSpace(m=8)
transport = InMemoryTransport(space)
a = ChordNode(Peer(20, "alice"), space, transport)
b = ChordNode(Peer(150, "bob"), space, transport)
transport.register(a)
transport.register(b)
a.create()
b.join(a.peer)

# A scheduler would call these once per maintenance tick, without real sleeps.
for _ in range(2 * space.m):
    for node in (a, b):
        report = node.maintain()
        # During transitions, report.errors may contain expected routing failures.

assert a.find_successor(100) == b.peer
result = a.lookup("an application key")
print(result.owner, result.hops, result.path)
```

This fixed example settles in the stated ticks; applications should not treat a
fixed tick count as a general convergence guarantee.

## Semantics

- IDs are integers in `[0, 2**m)`, with `1 <= m <= 256`. `node_id(identity)` and
  `key_id(key)` hash domain-separated UTF-8 strings or bytes using SHA-256, mapped
  to the low `m` bits. Small spaces can collide: duplicate node IDs are rejected,
  not silently reassigned. Use a stable identity independent of an endpoint.
- A node owns `(predecessor, node]`. All interval variants use a full clockwise
  turn for equal endpoints: `(a,a)` omits `a`; inclusive variants cover the ring.
- `create()` establishes an immediately valid singleton. `join(bootstrap)` learns
  a successor through distributed lookup and leaves other fingers unknown.
  Stabilization/notification establish neighbors; finger repair fills designated
  starts `(node + 2**i) % 2**m`. Only a converged ring promises correct ownership.
- `lookup_id(id)` returns immutable `LookupResult(owner, predecessor, path)`;
  `lookup(key)` hashes an application key first. `find_successor(id)` and
  `find_predecessor(id)` return peers. Peers hold both ID and endpoint.
- Lookups follow local closest preceding fingers and successor pointers. They
  never enumerate registered peers. They terminate within `max_hops` routing
  steps (default 512), or raise `LookupFailed` with a partial path. Unknown or
  unreachable immediate successors fail explicitly until maintenance repairs
  them. `hops` counts overlay edges, including the final edge to the owner; ping
  RPCs are separately counted by the simulated transport.
- `state` is a frozen snapshot with tuple fingers and successors, not mutable
  node internals. Finger zero always mirrors the primary successor. Higher
  fingers may be unknown/stale during joins and failures.

## Maintenance and failures

Call `stabilize()`, `check_predecessor()`, and `fix_fingers(index=None)` individually,
or `maintain()` to run all three. Omitted finger index rotates deterministically.
`maintain()` returns expected errors in `MaintenanceResult.errors`; programming
errors propagate. Host schedulers serialize calls, stop scheduling unavailable
nodes, and choose maintenance frequency. There are no threads or timers.

Successor lists default to four distinct clockwise peers, excluding self in a
multi-node ring. A failed successor is replaced by the nearest reachable local
list/finger hint. Stabilization then discovers closer neighbors. No reachable
hint means explicit failure, including a lone survivor of a former two-node ring.
`leave()` stops participation; neighbors use ordinary failure recovery. It does
not atomically splice neighbors or transfer application data.
In particular, a two-node survivor cannot infer singleton ownership after crash
or withdrawal. Restore an endpoint or explicitly rebootstrap; this is not an
atomic graceful-leave protocol. Failed finger repair retains its old entry but
advances the cursor to avoid starving other fingers. New join clears old lifecycle
hints and resets the cursor; hint version ordering applies within each lifecycle.

`InMemoryTransport.set_available(peer, bool)` simulates a crash/recovery while
preserving node state. `set_link(source, target, bool)` controls directed links.
Recovery of saved state is tested when the live ring remains connected; arbitrary
partitions, disjoint cycles, and unbounded churn have no healing guarantee.

## GossipMesh extension points

- Implement the `Transport` protocol (`ping`, `route`, `read_state`, `notify`) for
  another simulator or bounded network RPCs. Authenticate ID/endpoint bindings,
  reject mixed spaces, and translate reachability failures to `PeerUnavailable`.
- Supply `on_event: Callable[[Event], None]` for typed join/leave, neighbor changes,
  reachability hints, finger repairs, repair requests, and lookup outcomes. The
  callback is synchronous and should not raise or reenter the node; exceptions
  propagate. Without a callback, event history is not accumulated.
- `set_peer_available(peer, available, version=None)` is a reversible local hint.
  It does not change simulated transport availability or declare permanent death.
  Versioned hints ignore equal/older versions. GossipMesh should provide monotonic
  versions per peer (including incarnations); unversioned calls are authoritative
  local overrides. `True` permits probes again but does not install routes.
- `repair_peer(peer)` repairs affected fingers and the successor backbone and
  returns explicit errors. It is safe to repeat and does not infer peer death.
  Routing updates are only through validated protocol operations, not arbitrary
  public pointer setters. Periodic maintenance remains the baseline.

SWIM will own probes, suspicion, and membership dissemination. GossipMesh will
translate validated events into targeted repairs, deduplicate/rate-limit them,
and measure repair overhead. These layers are intentionally unimplemented.

## Paper alignment and limits

Based on Stoica et al., *Chord: A Scalable Peer-to-peer Lookup Service for Internet
Applications* (SIGCOMM 2001), sections 4–5, especially lookup Figure 4 and lazy
join/stabilization Figure 7. Differences: SHA-256 with hash domains instead of
SHA-1; zero-based fingers; iterative bounded lookup; deterministic rotating
finger repair instead of random indices; conservative no-route failures; local
finger fallback after list exhaustion; optional reachability hints/events.

This implements routing, not data replication, key migration, durability,
authentication, virtual nodes, or file transfer. Stable-ring invariants do not
apply atomically during joins/failures. A live peer with a stale successor may
return an obsolete owner during churn, as in the paper's lazy protocol. A local
algorithm cannot certify global ownership in an arbitrary partition. No Byzantine
or production network guarantees are claimed.

The experiment samples reproducible IDs/queries, runs batched joins until an
external oracle confirms neighbors and fingers, and reports hops, success rate,
finger correctness, maintenance operations, and RPC counts. It is a healthy-ring
scaling check, not a proof of logarithmic performance under arbitrary churn.
