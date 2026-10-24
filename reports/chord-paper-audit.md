# Source-to-paper audit: Chord (SIGCOMM 2001)

Reference: Stoica et al., *Chord: A Scalable Peer-to-peer Lookup Service for
Internet Applications*, attached `/home/aamer/Downloads/chord_sigcomm.pdf`.
Scope: original 2001 paper, not later strengthened Chord protocols. Audit date:
24 October 2026. Starting commit: `add23bd`, with existing local visualization,
report and documentation work preserved.

## Selected maintenance strategy

Section 4.4 / Figure 6 describes eager initialization and update_others for a
single join. Section 5.1 / Figure 7 replaces that approach with lazy successor
discovery plus stabilization/notification. This project selects **Section 5**.
It does not implement both strategies, key migration, or all paper optimizations.
An eager-join strategy would be separate future work, not an implicit guarantee.

| Mechanism | Paper | Source and audit conclusion |
| --- | --- | --- |
| Identifier mapping | §4.1–4.2: m-bit hashes, successor ownership | `chord/identifiers.py`: low-m-bit SHA-256 instead of SHA-1; separate node/key domains; stable identity independent of endpoint. Collision rejection at transport registration. |
| Circular ordering | §4: modulo 2^m intervals | `IdentifierSpace.contains/add/distance`: explicit full-turn equal endpoints; exhaustive independent enumeration tests. Production width does not enumerate the space. |
| Lookup | §4.3, Figure 4 | `ChordNode._lookup/route_step/find_successor/find_predecessor`: iterative remote steps and final successor; strict preceding intervals, monotonic progress, repeated-peer detection and finite budget. No registry ownership lookup. |
| Finger entries | §4.3, Table 1 | `state`, `finger_start`, `fix_fingers`: zero-based n+2^i starts; unknown fingers after join; entry zero mirrors successor; exact targets verified after convergence. |
| Create | §4.4 singleton case | `create`: successor/predecessor/self fingers immediately valid. Equal-endpoint intervals support whole-ring singleton ownership. |
| Join | §5.1, Figure 7 | `join`: bootstrap lookup supplies successor only; predecessor/higher fingers converge. Eager init_finger_table/update_others is not implemented. |
| Stabilize | §5.1, Figure 7 | `stabilize`: query successor predecessor, adopt a reachable closer candidate, refresh successor list and notify. Explicit local no-route failure rather than inventing singleton ownership. |
| Notify | §5.1, Figure 7 | `notify`: accept a reachable closer predecessor or fill an absent/unreachable predecessor. Duplicate/stale farther notifications do not displace a closer live predecessor. |
| Finger maintenance | §5.1, Figure 7 | `fix_fingers`: deterministic rotating index instead of random selection. Audit reproduced starvation on failed repair; regression fix makes rotation fair across failures. |
| Predecessor checking | Failure handling extension to Figure 7 | `check_predecessor`: transport liveness hint, not a SWIM detector. No permanent-death inference. |
| Successor lists | §5.2 | `stabilize/_set_successors`: distinct clockwise bounded peers, promote nearest reachable known successor. Local finger fallback is a documented extension. List length defaults to 4, not a theorem-derived log N parameter. |
| Leave/crash | §4.4 responsibility changes, §5.2 failures | `leave` performs clean local withdrawal; it does not atomically splice neighbors. Crash is transport unavailability with saved state. Both need periodic recovery and reachable local alternatives. Graceful neighbor-handoff/key transfer is future work. |
| Routing under failure | §5.2 alternate routes | Dead fingers are skipped; a dead immediate successor causes explicit lookup failure until stabilization. More conservative than the paper's alternate-path continuation discussion. No claim of the full failure theorems. |
| Integration hooks | Project extension | Typed events, reversible/versioned hints, repair_peer. They do not import failure-detector state or implement GossipMesh policy. |

## Findings before implementation changes

1. Rejoin defect: `join()` cleared lifecycle availability hints only after its
   bootstrap lookup. A stale local exclusion could reject an explicitly selected,
   currently reachable bootstrap. Reproduced with two nodes, hint false, leave,
   then join the live peer. Fix: discard old lifecycle hints before fresh joining.
2. Finger fairness defect: `_next_finger` advanced only after successful lookup.
   Three failed repairs attempted the same start. Fix: advance after validating
   the designated index and before attempting lookup; retain the old entry on
   failure, and emit a repaired event only on success.
3. Simulation gap: in-memory delivery and manual rounds existed, but no reusable
   virtual event queue or deterministic delay/drop model.
4. Measurement gap: original scaling harness measured one seed, mean/p95 hops
   and healthy convergence. It omitted tail/median latency, recovery traces,
   initialization message costs and memory observations.

## Assumptions and classification of limitations

- Stable-ring ownership is verified against an independent sorted-ID oracle.
  Correct successor pointers suffice for slower routing with stale fingers.
- During lazy joins/recovery, live stale successors may return obsolete owners.
  Benchmarks must count these separately from explicit failures; this is a
  transitional limitation, not something to hide by changing the oracle.
- Section 5 assumes eventual communication and maintenance after joins. Disjoint
  cycles and arbitrary partitions are not automatically healed by stabilization;
  the paper explicitly discusses this limit. Exhausting all local alternatives
  may require externally supplied bootstrap knowledge.
- The lone survivor of a former two-node ring cannot prove all peers permanently
  absent from reachability alone. Conservative no-route failure is intentional.
- A short successor list and extra local fallback do not establish the paper's
  probabilistic failure-resilience theorems. No general churn guarantee is claimed.
- No storage layer is included. Routing and ownership do not imply value
  persistence, migration, replication or fault-tolerant data recovery.

## Simulation interpretation

The added simulator uses a virtual clock and stable FIFO tie-breaking for scheduled
protocol actions. Synchronous RPCs consume explicit virtual service cost or a
timeout. Handlers execute serially/atomically; other scheduled callbacks run after
the current operation completes, even if their deadline elapsed during that call.
This deliberately matches the synchronous core. It supports scripted crashes,
recovery, directed links, deterministic drops and delays, but is not a model of
overlapping/reordered in-flight requests or real network latency.

The global oracle only observes snapshots and determines experiment stopping and
metrics. It never selects Chord routes or supplies successor/finger updates.
