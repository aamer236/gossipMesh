# Standard SWIM + GossipMesh: verification report

Date: 25 October 2026 (Asia/Kolkata). New branch: `chord_standard_swim`.
Starting base: `testing_plain_chord` at
`3c581ae6ebd96b920523632a58d30b19f7f28d3a`.
The checkout was clean before branch creation. The baseline implementation,
baseline benchmark runner, Compose file and local verifier remain byte-for-byte
unchanged. The `master` and `testing_plain_chord` branches were not modified.

## Files and architecture

- Added the independent `chord_standard_swim` Go module: reused Chord core,
  transport, compatible metrics and original Chord tests; strict startup config;
  SWIM membership/probing/HTTP transport; targeted repair APIs; coordinator;
  bounded event telemetry; SWIM and integration tests; a detailed run guide.
- Added `orchestrator/standard_swim_verify.py` and its oracle tests. It imports
  only observation/oracle helpers from the existing harness; it never injects
  routing state into participants or starts the baseline by mistake.
- Added the implementation plan, this report, current JSON samples/metrics and
  actual failure trace files. Updated the root README and binary/state ignores.
- No optimized finger-scoped variant or SWIM-specific Docker services were added.

The exact Chord APIs are `GetRoutingSnapshot`, `SetPeerMembershipHint`,
`MarkReferencesDirty` and `RepairPeerReferences`. Repair reuses
`handleSuccessorFailureIf`, `Stabilize`, `FindSuccessor`, `FingerStart` and
existing RPC validation rather than computing owners from membership.

## Mapping to the supplied papers

Chord uses Figures 4 and 7 and Section 5.2: lazy joins, closest-preceding routing,
clockwise successor lists, ordinary maintenance and lookup-based finger repair.
The variant preserves the independent audited baseline algorithm while adding
versioned liveness hints and guarded targeted repairs.

SWIM uses Section 3.1's direct/indirect probing, Section 4.1's bounded infection
piggybacks, Section 4.2's suspicion and incarnation refutation, and Section 4.3's
shuffled round-robin traversal of the **full active membership**, including
suspects. Dead tombstones are retained in the bounded full view and excluded
from probe targets. There is no finger-table restriction on SWIM target selection.

Every PING/ACK/PING_REQ is correlated with origin, round and target identity.
Helpers probe concurrently within fixed deadlines. Ordinary dissemination uses
existing probe/ACK traffic, not separate broadcast RPCs. Duplicates cannot reset
suspicion deadlines or retransmission budgets; a newer accepted update replaces
an older buffered update. One subject's newest pending update occupies one
buffer slot, and least-transmitted updates receive priority.

Original SWIM Confirm is terminal for a process instance. Stable-identity restart
is an explicit extension: self incarnations are persisted and increased;
bootstrap exchanges tombstones and re-advertises a newer ALIVE. Within an
incarnation the order is ALIVE < SUSPECT < DEAD; older incarnations are rejected,
and a valid newer ALIVE can supersede an older tombstone. Only the subject
creates its next ALIVE version; relays distribute that information in the trusted
(non-Byzantine) deployment model. HTTP/JSON request-response ACKs replace UDP.
These are documented choices, not a claim of byte-for-byte paper equivalence.

## Exact membership-to-routing path

1. A direct probe fails; bounded helpers try PING_REQ/ACK for the same round.
2. No valid ACK leads to SUSPECT. Gossip spreads it, while the subject can persist
   and advertise a higher-incarnation ALIVE refutation.
3. A local suspicion deadline expires without superseding information, or a
   confirmation arrives through gossip: the member becomes DEAD.
4. The membership callback runs outside its lock. The coordinator accepts the
   version once, publishes a reversible reachability hint and identifies all
   affected local roles.
5. SUSPECT only causes observation/validation. DEAD marks matching fingers dirty
   and queues one coalesced job per peer, prioritizing an immediate successor.
6. One worker promotes ping-validated local Chord hints, clears a dead predecessor,
   refreshes successor backups through stabilization and recomputes affected
   finger starts through distributed Chord lookups, four per batch.
7. The membership version and current successor are checked again after RPCs.
   Hint updates and routing writes share the short routing commit lock; candidates
   whose own DEAD hint arrived during probing are rejected before installation.
   This prevents a delayed death job from undoing a newer ALIVE or evicting an
   already-repaired live successor.
8. Completed writes, remaining work, retries and errors are recorded. Successful
   batches do not consume the failure-retry budget; retries stop after eight
   failures and ordinary maintenance remains active. All waits are outside global
   state locks. No goroutine is launched per finger.

## Commands actually run and results

Runtime: Go 1.26.8, Linux amd64. `go version -m` confirmed `-race=true` on the
acceptance binary. No external Go/Python packages were required.

| Command (module working directory where applicable) | Actual result |
| --- | --- |
| `go test -race -cover -v -timeout 90s ./...` in `chord_standard_swim/` | Passed; 26 top-level tests plus subtests; 77.8% statement coverage |
| `go vet ./...` in `chord_standard_swim/` | Passed |
| `go build -race -o /tmp/chord-standard-swim .` in `chord_standard_swim/` | Passed |
| `go test -race -cover -timeout 240s ./...` in `baseline_chord/` | Passed; 68.8% statement coverage |
| `python3 -m unittest orchestrator.test_standard_swim_verify orchestrator.test_runner -v` | All 12 tests passed |
| `ruff check` on the two new Python files | Passed |
| `git diff --check` | Passed |
| `git diff --exit-code testing_plain_chord -- baseline_chord orchestrator/runner.py orchestrator/docker-compose.yml orchestrator/local_verify.py` | No differences |

Tests cover direct ACKs, indirect success after direct failure, suspicion before
confirmation, self-refutation, expiry, duplicate/reordered/stale updates, bounded
buffers/retirement, full-view target coverage, stale ACKs, endpoint binding,
persistence/restart reconciliation, malformed HTTP payloads, cancellation and
orderly runtime shutdown. Chord tests cover reversible suspicion, all affected
fingers, successor recovery, duplicate job coalescing, concurrent maintenance,
large dirty sets requiring more than eight successful batches, and a combined
false-suspicion/refutation flow through real SWIM events and Chord APIs.

Deterministic blocked-probe tests specifically cover two races: a newer ALIVE
arriving while a death repair probes candidates, and a candidate becoming DEAD
while its own ping is in flight. The former preserves the refuted successor; the
latter refuses to install the newly dead candidate.

## Current six-process continuous-query run

Command:

```sh
python3 -u -m orchestrator.standard_swim_verify \
  --binary /tmp/chord-standard-swim --nodes 6 \
  --output reports/standard-swim-results.json
```

Configuration: m=8, R=4, 3,000 ms stabilization/predecessor checks, 500 ms ordinary
finger refresh, 200 ms SWIM interval, 50 ms direct timeout, two helpers and
1,200 ms suspicion. Full membership and every routing-table entry were checked
against the oracle before stable phases. A hard process kill removed ID 128.

| Phase | Oracle correct / queries | p50 ms | p95 ms | p99 ms | Mean hops |
| --- | --- | --- | --- | --- | --- |
| healthy | 1,536 / 1,536 | 5.374 | 10.211 | 16.526 | 2.155 |
| recovery | 1,260 / 1,260 | 6.025 | 13.978 | 18.195 | 1.970 |
| post_crash | 1,280 / 1,280 | 4.424 | 8.226 | 10.245 | 2.055 |
| restart | 1,536 / 1,536 | 4.983 | 10.217 | 15.707 | 2.155 |

All **5,612** lookups were correct. Stable m=8 phases checked every one of
the 256 keys from every initiator; recovery used repeated boundary/random batches.
The >=99% threshold in two consecutive batches was observed at
**0.322 s** after the kill. SWIM's first suspicion was
**0.010 s**, first confirmation
**1.255 s**, and the observed spread
between first and last confirmation was **0.165 s**.
All five survivors learned the death. Restarting the same identity/address used
incarnation **1** and restored the full ring/view.

The query path and ordinary Chord maintenance are intentionally active: sampled
routing recovered before SWIM confirmed death. This is **not evidence of a SWIM
speed advantage over baseline**. The coordinator still performed
6 validated targeted finger writes; the trace also records
1 actual successor promotion(s).
All processes exited; the final cohort logged 6 orderly shutdowns,
with the killed instance's replacement counted as the restarted participant.

## Separate proactive run and actual failure trace

Command:

```sh
python3 -u -m orchestrator.standard_swim_verify \
  --binary /tmp/chord-standard-swim --nodes 4 --m 4 \
  --fix-ms 3000 --observe-ms 2000 \
  --output reports/standard-swim-proactive-results.json
```

The 2-second query-free observation window exposes proactive work; it makes the
subsequent sampled recovery time a delayed observation, not a faster/slower
comparison with the continuous run. Ordinary Chord maintenance remains active.
The ring contains IDs 0, 4, 8, 12; ID 8 is killed and restarted.

| Phase | Oracle correct / queries | p50 ms | p95 ms | p99 ms | Mean hops |
| --- | --- | --- | --- | --- | --- |
| healthy | 64 / 64 | 6.279 | 14.046 | 17.451 | 1.812 |
| recovery | 72 / 72 | 7.008 | 24.259 | 28.814 | 1.583 |
| post_crash | 48 / 48 | 4.304 | 8.925 | 10.514 | 1.604 |
| restart | 64 / 64 | 7.740 | 12.523 | 17.613 | 1.812 |

All **248** queries were correct. The trace records
1 actual successor promotion(s),
0 coordinator successor validation(s), and
3 targeted finger writes. A representative actual sequence at node 4:

```text
t+0.099s local=4 target=8 peer_suspected {"incarnation":0,"previous":"ALIVE","state":"SUSPECT"}
t+1.341s local=4 target=8 successor_promoted {"address":"127.0.0.1:45015","cause":"validated_chord_fallback","confirmed_dead_hint":false,"successor":"12"}
t+1.343s local=4 target=8 peer_dead {"incarnation":0,"previous":"SUSPECT","state":"DEAD"}
t+1.343s local=4 target=8 membership_event_handled {"event":"peer_dead","incarnation":0,"state":"DEAD"}
t+1.343s local=4 target=8 references_identified {"affected":{"fingers":[1,2],"predecessor":false,"successor":false,"successor_list_entries":0},"state":"DEAD"}
t+1.347s local=4 target=8 finger_repaired {"index":1,"owner":"12","start":"6"}
t+1.348s local=4 target=8 finger_repaired {"index":2,"owner":"12","start":"8"}
t+1.349s local=4 target=8 repair_completed {"attempt_ms":4.259,"incarnation":0,"latency_ms":5.737,"result":{"pending":false,"repaired":2,"successor_promoted":false},"state":"DEAD"}
```

The complete source events are in `standard-swim-proactive-results-trace.jsonl`;
no synthetic failure trace is substituted for process output. The generic
`successor_promoted` event records the exact committed Chord promotion, while
`successor_repair_validated` records the coordinator's validation, avoiding double
counting the promotion itself. The same endpoint restarted with incarnation
1 and the ring converged again. All processes exited cleanly.

Node 0 confirmed the death first, at t+1.333 s, and began its targeted finger
lookup for key 8. A distributed lookup can invoke the shared Chord failure
fallback at another participant before that participant's own SWIM confirmation
arrives. The trace's node-4 promotion at t+1.341 s therefore precedes node 4's
local death event, while its remaining finger repairs follow that event. These
logs distinguish the common Chord promotion point from a coordinator's own
successor validation; they do not attribute every background change exclusively
to a local SWIM event.

## Observed counters and memory

The following are **cumulative counters of the five surviving processes at the
post-crash sample**, not normalized phase deltas or a baseline comparison:

| Counter | Sum across survivors |
| --- | --- |
| `direct_probes` | 882 |
| `direct_failures` | 9 |
| `indirect_probes` | 18 |
| `probe_timeouts` | 0 |
| `indirect_timeouts` | 0 |
| `suspicions` | 5 |
| `refutations` | 0 |
| `confirmed_failures` | 5 |
| `update_piggybacked` | 465 |
| `update_received` | 4,015 |
| `references_invalidated` | 9 |
| `references_repaired` | 6 |
| `swim_messages_sent` | 1,828 |
| `swim_payload_bytes_sent` | 600,531 |

Each surviving node still had 6 membership records, including the dead
tombstone; exactly one was DEAD in each view. Mean process heap allocation at
that observation was **2364.3 KiB**. This includes Go runtime, HTTP,
telemetry history, SWIM and Chord allocations; it is not an isolated SWIM heap
measurement. Membership-record counts and payload/RPC counters are included
in the raw JSON for each phase. The two acceptance cohorts ran concurrently
with race instrumentation, so latency values are diagnostic observations, not
controlled production performance results.

`references_invalidated` counts affected routing roles; overlapping successor,
finger-zero and list entries are distinct roles. `references_repaired` is a
conservative count of validated coordinator writes, with list refresh and ordinary
Chord changes separately observable. These are not a global repair percentage.
Repair-latency percentiles cover completed jobs with validated writes, measured
from enqueue through completion, including queue/retry time. API lookup success
is not mathematical correctness; the external oracle supplies the latter.

## Artifacts and limits

- [Continuous run: raw samples, metrics and configuration](standard-swim-results.json)
- [Continuous failure trace](standard-swim-results-trace.jsonl)
- [Proactive run: raw samples, metrics and configuration](standard-swim-proactive-results.json)
- [Proactive failure trace](standard-swim-proactive-results-trace.jsonl)
- [Build, run, configuration and protocol guide](../chord_standard_swim/README.md)
- [Pre-implementation audit/plan](standard-swim-implementation-plan.md)

Both final runs used the same actual race-enabled binary, SHA-256:
`5b102b00cd50ad9fc5624cad4bdbb1dbb0e26e12b73c81c351663bde42c75ab0`.
The reports were generated by these runs, not copied from pre-existing metrics.

This establishes a working local full-membership SWIM/Chord integration under
the tested connected-ring conditions. It does not prove every paper theorem,
asymptotic behavior, arbitrary churn correctness or partition healing. Timing
parameters need tuning for larger/slower groups. Transient lookup errors/obsolete
owners remain possible while Chord is converging.

False suspicion was tested using controlled traffic loss, separately from the
hard process crash. Sustained loss can still produce false confirmation. Gossip
is bounded best effort; updates can be lost after their retransmission budget or
buffer pressure. There is no periodic full-view anti-entropy. Tombstones consume
membership capacity until operator-managed replacement/rejoin; indefinite new-ID
churn can exhaust the configured full-view bound.

Restart requires a reachable live bootstrap and a stable endpoint. The original
bootstrap must be pointed at a survivor when it restarts without a peer view.
Changing an existing identity's address, losing all incarnation/tombstone state,
or healing disjoint rings is not guaranteed. Self persistence is for ordinary
process restarts, not power-loss or multi-writer state-file recovery.

The HTTP transport is not the paper's UDP implementation. Payload counters are
attempted/submitted body bytes, not captured TCP/IP wire traffic. Identity/address
and correlation validation assumes honest participants; cryptographic identity,
Byzantine protection, storage replication and key migration are not implemented.
The optimized scoped-SWIM variant is intentionally absent.

Docker/Compose and real `tc netem` delay injection were not exercised in this
milestone. The unchanged Docker benchmark still selects baseline Chord. No
Docker or deployment success is claimed.
