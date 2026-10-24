# Go Chord: paper alignment and verification

Audit date: 25 October 2026. Base commit: `9daaabb` on `testing_plain_chord`.
Source: the repository's `Chord.pdf`, Stoica et al., SIGCOMM 2001, especially
Sections 4.2-4.3, Figure 4 (printed p.153), Section 5.1/Figure 7 (p.155),
Section 5.2 (p.156), and Section 7 (p.160).

## What the paper requires

The routing owner is the first node clockwise at or after a key. Finger i in this
zero-based implementation points at successor(n + 2^i mod 2^m). Finger zero is
the immediate successor, not an independently maintained route. Figure 4 forwards
toward the target using the closest preceding finger; successors remain enough
for correct, slower lookups when higher fingers are inaccurate.

Section 5.1 replaces Section 4's eager join with a lazy join that initially learns
only a successor. Stabilization learns closer successors via predecessor queries,
then notifies the selected successor. Figure 7 randomly refreshes fingers other
than the successor. Implementing the earlier eager `update_others` algorithm is
not required in addition to this algorithm.

Section 5.2 maintains the r nearest clockwise successors and promotes a live
backup on failure. Failed routing hops can use alternate fingers/successors.
The paper does not authorize an unreachable node to declare itself the only
participant. Its high-probability failure results assume an initially stable
ring, randomly distributed failures, and a successor-list length O(log N).

The paper explicitly permits transient lookup failures during stabilization and
does not claim that ordinary stabilization heals arbitrary partitions/disjoint
cycles. Correctness at convergence must not be confused with instantaneous global
ownership knowledge during churn.

## Protocol fixes

| Area | Previous behavior | Fixed behavior |
| --- | --- | --- |
| Exhausted routing | Returned self with nil error | Explicit routing error when no owner can be determined |
| Successor failure | Discarded the ring and declared a singleton | Tests clockwise successor/finger hints; preserves unknown ownership when no route remains |
| Successor list | On adopting a closer successor, used the old successor's list and could omit the old successor itself | Reads the chosen successor's list and retains the old successor as a backup, ordered/deduplicated and bounded by R |
| Finger zero | Changed separately from the successor | Every successor update also updates finger zero |
| Finger refresh | Rotated through every finger, including finger zero | Figure 7's random refresh of non-successor fingers; stabilization owns finger zero |
| Lazy join | Left higher fingers pointing to the joining node itself | Clears higher fingers until distributed repair learns them |
| RPC cycles | No shared path/hop bound | Tracks the request path, rejects cycles and caps it at 512 participants |
| Concurrent state changes | Could install a stabilization/failure result after another operation had replaced its successor | Applies the result only if the successor that was queried is still current |
| Failure probing | Held the node-state write lock during remote pings | Probes outside the state lock, then checks state again before committing |
| Identifiers/references | Accepted invalid IDs, missing candidate IDs, duplicate join IDs and mismatched identifier spaces | Rejects malformed/out-of-range identities, duplicate join IDs and peers using a different M |

The RPC path/hop bound, explicit validation, nil/error handling, and concurrency
guards are engineering safeguards rather than literal pseudocode statements.
Routing still uses only each participant's own protocol state. The benchmark
oracle never supplies routes to nodes.

## Operational and benchmark fixes

- HTTP listener binds synchronously so startup returns occupied-port errors.
- Incoming RPC body bytes and outgoing response bytes now contribute to telemetry;
  error response payloads are also counted. These are body-payload counters,
  not a packet capture or HTTP/TCP wire-byte measurement.
- The runner waits for two consecutive snapshots with correct neighbors, all
  fingers and the full expected successor lists. It has a five-minute deadline
  rather than silently starting after eight seconds. With random refresh, one
  sweep duration is not enough to guarantee every finger has been visited.
- Recovery probes run across all surviving initiators, include ownership
  boundaries and random keys, and check owner ID and address against an oracle.
  Two batches must each reach 99% correctness. This is an operational sampling
  definition, not the paper's theorem or a statistical proof for the full keyspace.
- Recovery has a monotonic deadline and explicit timeout status with a null time;
  CSV exports `TIMEOUT`. Overall output includes `verification_passed`.
- Fault injection now uses Docker's KILL signal for the claimed hard crash;
  the former `docker stop` exercised the application's graceful signal handler.
- Percentiles use the same nearest-rank definition in Go and Python.
- Saved pre-fix benchmark results are flagged as requiring regeneration.

## Verification

Runtime: Go 1.26.8 on Linux. Commands from the repository root:

```sh
(cd baseline_chord && go test -race -cover -v -timeout 240s ./...)
(cd baseline_chord && go vet ./...)
python3 -m unittest orchestrator.test_runner -v
```

All Go tests passed with the race detector, with **69.0% statement coverage**.
All **eight Python tests** passed. The Python checks include a reproduction where
every reply claims success but names the wrong owner; it now times out rather
than reporting recovery. They also cover incomplete tables, missing survivors,
wrong owner addresses, successful recovery and timeout exports through main.

The Go tests cover the original six reproduced regression failures, the original
test suite, cycle rejection, duplicate/mixed-space joins, successor/finger-zero
consistency, correct backup construction, and state reads during a blocked ping.

After concurrent joins and oracle-checked full table convergence, every key in an
8-bit identifier space was looked up from every node. Two adjacent nodes then
failed and the checks repeated after convergence:

| Initial nodes | Healthy correct/total | After two crashes correct/total | Healthy mean/max hops | Post-crash mean/max hops |
| --- | --- | --- | --- | --- |
| 8 | 2,048 / 2,048 | 1,536 / 1,536 | 2.35 / 4 | 2.17 / 3 |
| 16 | 4,096 / 4,096 | 3,584 / 3,584 | 2.96 / 6 | 2.89 / 5 |
| 32 | 8,192 / 8,192 | 7,680 / 7,680 | 3.35 / 6 | 3.31 / 6 |

These are **27,136** successful checks. Additional Go checks contribute 3,584
lookups in the regression ring, 1,024 under concurrent maintenance, and 2,048
after saved-endpoint restoration and explicit rejoin: **33,792** successful
lookup assertions in total. Zero-hop self lookups are counted as zero overlay
edges. The observed hop growth supports the expected routing behavior but is
not a proof of asymptotic complexity.

The separate local process verifier uses eight race-enabled Go executables,
m=160, R=4, hashed endpoint identities, and accelerated 10/5/15 ms maintenance.
It checks full routing-table convergence before each query phase, injects two
adjacent process kills and restarts the killed processes. Its result is recorded
in `reports/go-chord-local-verification.json`.

## Assessment and remaining limits

For the tested connected-ring conditions, the implementation now follows the
paper's lookup, lazy stabilization and successor-list recovery design. All
reproduced correctness/validation/measurement errors have regression coverage.

This does not prove every theorem or certify arbitrary churn/failure schedules.
The 512-hop cap, finite HTTP timeouts and local failure hints can produce explicit
errors before maintenance repairs a route. During joins or failures, stale
successor pointers can still yield obsolete ownership as permitted by the lazy
protocol; there is no global ownership certificate. Losing all useful local hints
requires restoring contact or explicitly rejoining, not inventing a singleton.

The fixed R=4 benchmark setting is not a universal scaling guarantee; R must
grow with the intended N for the paper's probabilistic analysis to apply. Tests
at 8-32 nodes are evidence, not large-scale statistical verification.

This is a routing implementation. It does not implement data storage, key/value
migration, application ownership-change callbacks, data replication, virtual-node
load balancing, authentication or automatic partition healing. HTTP/recursive
RPC is an implementation choice; Section 6.1 permits iterative or recursive
implementations. SHA-1 and m-bit circular identifiers match Section 4.2, with
configuration restricted to 1 <= m <= 160 to stay within the hash width.

Docker's daemon is unavailable in this environment. The container build, Compose
lifecycle and actual `tc netem` gray-failure experiment remain unverified. The
committed pre-fix benchmark numbers have not been regenerated or endorsed.
