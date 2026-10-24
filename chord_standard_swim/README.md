# Chord + standard SWIM + GossipMesh

This is the full-membership variant. Every participant tracks the whole known
SWIM group, including death tombstones, and probes across that view. Probe
selection is independent of Chord's fingers and successor list.

Base: `testing_plain_chord` at `3c581ae6ebd96b920523632a58d30b19f7f28d3a`.
`chord.go`, `network.go`, `metrics.go` and the original Chord tests were reused
from that base in this separate Go module. The small routing hooks are in
`routing_repair.go`. No routing algorithm is implemented inside SWIM, and
`baseline_chord/` remains unchanged and independently runnable. This explicit
variant copy follows the repository's isolated-case architecture; future Chord
fixes should be reviewed for both variants.

## Build and run

Requires Go 1.20+; race-enabled builds require a working C toolchain. The local
verifier uses Python 3.11+ and its standard library. No external Go modules or
Python packages are needed.

From the repository root:

```sh
(cd chord_standard_swim && go build -race -o /tmp/chord-standard-swim .)

# Terminal 1: first participant
PORT=8100 ADVERTISE_ADDR=127.0.0.1:8100 NODE_ID=20 M=8 \
  SWIM_STATE_FILE=/tmp/swim-node-20.json /tmp/chord-standard-swim

# Terminal 2: join via the first participant
PORT=8101 ADVERTISE_ADDR=127.0.0.1:8101 NODE_ID=120 M=8 \
  BOOTSTRAP_ADDR=127.0.0.1:8100 SWIM_STATE_FILE=/tmp/swim-node-120.json \
  /tmp/chord-standard-swim

curl http://127.0.0.1:8100/health
curl http://127.0.0.1:8100/membership
curl http://127.0.0.1:8100/state
curl 'http://127.0.0.1:8100/lookup?id=90'
curl http://127.0.0.1:8100/metrics
curl http://127.0.0.1:8100/events
```

Use SIGTERM or Ctrl-C for orderly shutdown. `/health` returns 503 until local
SWIM bootstrap and Chord join complete; it is not a global convergence test.
All nodes must use the same identifier width, identity/address bindings and
compatible membership payload limits. Chord ownership is promised only after
convergence, subject to the connected-ring assumptions below.

## End-to-end verification and benchmark

```sh
python3 -m orchestrator.standard_swim_verify \
  --binary /tmp/chord-standard-swim --nodes 6 \
  --output reports/standard-swim-results.json

# Separate proactive demonstration: allow death detection before querying.
python3 -m orchestrator.standard_swim_verify \
  --binary /tmp/chord-standard-swim --nodes 4 --m 4 \
  --fix-ms 3000 --observe-ms 2000 \
  --output reports/standard-swim-proactive-results.json
```

The verifier reserves random loopback ports, starts real processes with distinct
explicit IDs, and waits for the full membership view plus oracle-correct Chord
neighbors, all fingers and successor lists. It queries every key for m <= 8,
kills a multiply referenced peer, samples correctness through recovery, checks
death dissemination, verifies the surviving ring, restarts the same identity and
checks convergence and lookups again. It terminates survivors and rejects panic,
race reports or forced shutdowns.

The continuous benchmark's recovery threshold is **at least 99% oracle-correct
responses in two consecutive batches, from every survivor**, using node-ID
boundaries, preceding keys and 16 seeded random draws. This is sampled recovery,
not a theorem about all keys or the exact instant the ring became correct.
Post-convergence m <= 8 phases separately check the entire keyspace from every
initiator. Failed responses and incorrect successful responses are distinguished
in raw samples. The proactive run deliberately waits before its first lookup;
its recovery observation is therefore delayed and must not be compared directly
with continuous-query recovery times.

Fresh JSON includes per-query samples, phase metrics, memory/runtime statistics,
membership-record counts, detection/confirmation timing, observed confirmation
spread and restart incarnation. A companion `*-trace.jsonl` records the actual
failure/repair events. The verifier requires recorded death, coordinator handling
and targeted finger-repair events before declaring acceptance. Ordinary Chord
maintenance and lookup-triggered recovery stay active; a fast recovery does not
by itself prove that SWIM caused it or outperformed baseline Chord.

Docker is not required. The baseline Compose file and Docker benchmark are
unchanged; they do not run this variant. No SWIM-specific Docker benchmark is
claimed verified in this milestone.

## Protocol and integration

`membership.go` owns immutable identity/address bindings, membership records,
incarnation ordering, suspicion deadlines, tombstones, target traversal and the
bounded dissemination buffer. `swim.go` owns probing and lifecycle management.
`swim_http.go` implements the versioned `/swim/rpc` endpoint on the same listener
as Chord. JSON/HTTP request-response ACKs replace the paper's UDP messages.

Each probe ID combines initiator identity, its incarnation and a monotonic local
sequence. Direct ACKs must match the round, origin, responding endpoint and
target. After direct failure, up to K helpers probe the target concurrently and
return an ACK witness for that same target/round. Cancellation and the indirect
deadline bound the round. Helper request handlers have a separate 32-request cap.

Targets traverse the complete active membership in shuffled round-robin order.
Suspects remain eligible; confirmed-dead entries are excluded. Newly learned
peers enter the remaining traversal at a random position. No finger-scoped
selection is implemented.

Failed direct and indirect probes produce SUSPECT. First acceptance starts a
local suspicion deadline; duplicate suspicion cannot extend it. A newer valid
ALIVE supersedes that suspicion, so expiry cannot confirm an older incarnation.
Only accepted DEAD transitions count as confirmed failures. Clock synchronization
is not used for version ordering or suspicion expiry.

The same-incarnation order is `ALIVE < SUSPECT < DEAD`; older incarnations are
ignored. A subject that learns it is suspected persists a higher self incarnation
before publishing ALIVE. An ACK carrying an older/equal ALIVE cannot erase a
current suspicion just because it arrived later. Newer ACK observations prevent
an older failed probe from creating a suspicion.

Updates piggyback on PING, PING_REQ and ACK traffic. The buffer holds one newest
update per subject, prefers least-transmitted updates, and retires an update
after `factor * ceil(log2(known_members + 1))` transmissions. Capacity pressure
retires older/more-transmitted entries and records that retirement. Ordinary
dissemination never broadcasts independent update RPCs. JOIN alone exchanges a
full membership snapshot; there is no periodic full-table anti-entropy protocol.

`controller.go` consumes versioned membership events outside the membership lock:

1. Publish a reversible local reachability hint; do not install routes from SWIM.
2. For SUSPECT, record affected references and validate liveness without eviction.
3. For DEAD, identify successor, predecessor, list and every matching finger.
4. Coalesce jobs by peer and prioritize an affected immediate successor.
5. One worker invokes `RepairPeerReferences`, promoting a ping-validated local
   successor/finger hint through existing `handleSuccessorFailure`, then running
   ordinary `Stabilize` to refresh backups.
6. Recompute dirty fingers with `FindSuccessor(FingerStart(node, index, M))` and
   validate the returned peer. Four fingers per batch bound worker monopolization.
7. Check state/version again before committing a result; a newer ALIVE clears the
   dead hint and supersedes stale queued work. Failed jobs get up to eight retries;
   successful partial batches do not consume that retry budget. Periodic Chord
   maintenance remains a fallback for unresolved work.

Membership-hint writes share the routing commit lock. A job also rechecks its
version after probing and rejects candidates whose own death hint arrived while
the ping was in flight. Deterministic blocked-probe tests cover both cases.

DEAD is an imperfect reachability hint, not proof of the mathematically correct
owner. Confirmed-dead hints bypass repeated Chord RPCs to that endpoint, but all
replacement routes come from Chord's local pointers and lookup algorithm.
SUSPECT does not disable the endpoint. Ordinary Chord timeout recovery may still
change pointers independently of the coordinator while suspicion is outstanding.
No remote operation holds the global membership or routing-state lock.

## Restart policy

Original SWIM Confirm is terminal for a process instance. Supporting the same
stable Chord identity after restart is an explicit extension: higher-incarnation
ALIVE can supersede an older DEAD tombstone. The endpoint must remain unchanged.
Each restart advances the self incarnation from a local state file; self
refutations also persist before publication. Bootstrap exchanges the full view,
including tombstones, then advertises a version above any previously known self
version. This also works after losing the local file if a bootstrap still holds
the old tombstone. One process must own each identity/state file.

Use a reachable surviving bootstrap when restarting. The original bootstrap
cannot discover survivors from nothing after losing its membership view; restart
it with `BOOTSTRAP_ADDR` set to a survivor. A process with no bootstrap explicitly
creates a new singleton. Identity/address rebinding is rejected; moving an
identity to a different endpoint is not implemented.

Tombstones remain in the full view, within `SWIM_MAX_MEMBERS`, to reject stale
messages. They do not remain in the retransmission buffer indefinitely. An
indefinitely churning group can reach the configured membership capacity and
require operator intervention; no unsafe tombstone garbage collection is used.
The state file is atomic for ordinary process restarts; power-loss recovery and
multi-process sharing of a state file are not guaranteed.

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `PORT` | 8000 | Shared HTTP listener |
| `ADVERTISE_ADDR` | 127.0.0.1:PORT | Stable advertised host:port |
| `NODE_ID` | SHA-1(address) mod 2^M | Stable decimal Chord identity |
| `BOOTSTRAP_ADDR` | empty | Reachable existing participant; empty creates a ring |
| `M` | 160 | Identifier bits, 1..160 |
| `R` | 4 | Successor-list capacity |
| `STABILIZE_INTERVAL_MS` | 200 | Ordinary Chord stabilization |
| `FIX_FINGERS_INTERVAL_MS` | 100 | Ordinary random non-successor finger refresh |
| `CHECK_PRED_INTERVAL_MS` | 300 | Ordinary predecessor probing |
| `RPC_TIMEOUT_MS` | 500 | Chord RPC timeout |
| `SWIM_INTERVAL_MS` | 600 | Protocol interval, at least 3 direct timeouts |
| `SWIM_DIRECT_TIMEOUT_MS` | 100 | Direct/helper target-probe timeout |
| `SWIM_INDIRECT_HELPERS` | 3 | Helpers, 0..8 |
| `SWIM_SUSPICION_TIMEOUT_MS` | 3000 | At least 2 protocol intervals |
| `SWIM_MAX_PIGGYBACK` | 6 | Maximum queued updates per outgoing message, 1..64 |
| `SWIM_MAX_UPDATES` | 256 | Bounded retransmission buffer; at least piggyback limit |
| `SWIM_MAX_MEMBERS` | 4096 | Full-view records, including tombstones; 2..16384 |
| `SWIM_RETRANSMIT_FACTOR` | 4 | Logarithmic update retransmission multiplier |
| `SWIM_STATE_FILE` | /tmp/gossipmesh-swim-ID-PORT.json | Persistent self-incarnation file |
| `LOG_LEVEL` | info | debug, info, error or quiet |

Startup validates IDs, addresses, bounds and timing relationships. Timeouts must
be tuned to actual scheduling and network latency; aggressive settings can
produce false suspicions and confirmations. R and suspicion/retransmission
parameters should be chosen for the intended group size, not treated as universal
constants for arbitrarily large deployments.

## Observability and tests

Structured events include local/target IDs, timestamp, event type and outcomes.
`/events` retains the most recent 2,048 events; `LOG_LEVEL=debug` also prints
per-probe and per-update details. Cumulative counters are available under
the `swim.counters` field of `/metrics`; membership sizes are under its `swim` field, and repair
job/latency statistics under its `gossipmesh` field.

Network counters cover attempted JSON/body payloads submitted by senders and
consumed by receivers. Failed sends may never reach their destination; these
are not TCP/IP wire-byte measurements. Chord and SWIM counters are separate.
Membership `update_received` includes merge attempts for sender advertisements;
`update_piggybacked` counts queued-update transmissions. ALIVE/SUSPECT/DEAD
transition counters are deduplicated by version.

`references_invalidated` counts affected routing roles, including overlapping
successor/finger-zero/list references. `references_repaired` conservatively counts
validated coordinator writes; list refresh/removal has separate events. They are
not a global repair percentage. Repair latency measures event submission through
completion of a job with validated writes, including queue/retry time; per-attempt
time is also logged. A refreshed list means locally ordered, reachable backups,
not an oracle proof of every global neighbor. Lookup `success_rate` is RPC-level
success; only the external verifier measures mathematical owner correctness.

```sh
(cd chord_standard_swim && go test -race -cover -v -timeout 90s ./...)
(cd chord_standard_swim && go vet ./...)
(cd baseline_chord && go test -race -cover -timeout 240s ./...)
python3 -m unittest orchestrator.test_standard_swim_verify orchestrator.test_runner -v
```

Unit tests use injected time and controlled transports for expiry, reordering,
direct/indirect failure and refutation. Integration tests use real Chord HTTP
servers and exercise concurrent maintenance, bounded repair batches, coalescing,
false suspicion, restart and runtime shutdown. The process verifier is a separate
acceptance layer. Current results and limitations are in
`../reports/standard-swim-verification.md`.

SWIM is an unreliable failure detector: false confirmations, sustained partitions,
lost gossip beyond retransmission budgets and exhausted local Chord hints have no
unconditional healing guarantee. Explicit rejoin may be required after a false
confirmation has fully retired from gossip. HTTP/TCP differs from the paper's
UDP implementation; no identical packet cost or performance theorem is claimed.
Payload identities are checked against stable bindings and round targets, but
there is no cryptographic authentication or Byzantine protection. There is no
storage replication, key migration, durable data layer or finger-scoped SWIM.
