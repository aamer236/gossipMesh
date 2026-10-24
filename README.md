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

No installation is required. To install as a package, use `pip install .`.
Development tools are optional: `pip install -e '.[dev]'`.

```sh
.venv/bin/ruff format --check chord tests experiments
.venv/bin/ruff check chord tests experiments
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
