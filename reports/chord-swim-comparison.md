# Chord / Chord + standard SWIM comparison

Generated 2026-10-24T22:24:11.236165+00:00. Acceptance passed: True.

Same offered lookup schedule, IDs, Chord timers, gray fault and hard-crash victim. Runs execute sequentially with alternating order.

Values below are medians of per-run results. Raw samples, ranges, counters and SWIM events are in the companion JSON.

| Phase | Variant | Runs | Correct % | p50 ms | p95 ms | p99 ms | Mean hops | Recovery s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| healthy | chord | 3 | 100.000 | 5.313 | 17.125 | 30.203 | 1.750 | — |
| gray_failure | chord | 3 | 100.000 | 6.619 | 111.403 | 121.283 | 1.750 | — |
| gray_recovered | chord | 3 | 100.000 | 5.310 | 19.001 | 26.333 | 1.750 | — |
| crash_recovery | chord | 3 | 100.000 | 5.219 | 11.555 | 21.255 | 1.604 | 2.000 |
| post_crash | chord | 3 | 100.000 | 4.852 | 13.515 | 23.395 | 1.604 | — |
| restart | chord | 3 | 100.000 | 5.359 | 11.955 | 22.931 | 1.750 | — |
| healthy | chord_standard_swim | 3 | 100.000 | 5.835 | 21.685 | 29.395 | 1.750 | — |
| gray_failure | chord_standard_swim | 3 | 100.000 | 8.225 | 112.310 | 123.265 | 1.750 | — |
| gray_recovered | chord_standard_swim | 3 | 100.000 | 5.169 | 17.736 | 28.629 | 1.750 | — |
| crash_recovery | chord_standard_swim | 3 | 100.000 | 5.080 | 12.493 | 24.225 | 1.604 | 2.000 |
| post_crash | chord_standard_swim | 3 | 100.000 | 5.242 | 11.662 | 17.846 | 1.604 | — |
| restart | chord_standard_swim | 3 | 100.000 | 5.247 | 13.628 | 24.517 | 1.750 | — |

Fault: 100% of incoming peer RPCs to one live victim delayed before forwarding; client lookups/management bypass proxy; no packet loss; after healing kill same victim.

Recovery: two consecutive one-second scheduled cohorts >=99% oracle-correct per survivor; completion time included; null means not observed within fixed window.

Traffic: destination proxy request attempts and JSON body bytes, including SWIM; no HTTP/TCP/IP overhead; proxy errors include client disconnects.

Gray-failure oracle includes the delayed live node. Only the hard-crash phase removes that node from the oracle. Gray recovery is checked before the crash; restart uses the same identity/address and persisted SWIM incarnation.

SWIM events/metrics have no baseline equivalent and are null for Chord. Whole-process memory snapshots are provided, not SWIM-only memory. Lookup latency includes failed requests; mean hops includes correct requests only.

- Local proxy overhead affects both variants; not equivalent to netem packet delay.
- Small-ring sampled lookup results do not prove arbitrary-ring or partition correctness.
- Sequential alternating paired runs; no statistical significance claim.
- Null metrics are unsupported/unobserved, never inferred zero.

## Peer traffic and memory

Per-phase medians across runs. Traffic is measured at proxies; Go heap is a whole-process snapshot averaged over active nodes. Neither includes proxy Python memory.

| Phase | Variant | Peer requests | Body bytes | Mean Go heap KiB |
|---|---|---:|---:|---:|
| healthy | chord | 622 | 38382 | 2740.2 |
| gray_failure | chord | 622 | 38075 | 1463.4 |
| gray_recovered | chord | 624 | 38709 | 2288.1 |
| crash_recovery | chord | 725 | 39069 | 1710.4 |
| post_crash | chord | 482 | 26244 | 1772.3 |
| restart | chord | 628 | 38944 | 2063.9 |
| healthy | chord_standard_swim | 700 | 87000 | 1919.3 |
| gray_failure | chord_standard_swim | 843 | 154490 | 2124.1 |
| gray_recovered | chord_standard_swim | 707 | 90892 | 1953.1 |
| crash_recovery | chord_standard_swim | 868 | 121499 | 2265.7 |
| post_crash | chord_standard_swim | 542 | 63026 | 2355.1 |
| restart | chord_standard_swim | 714 | 88482 | 2144.2 |

The recovery statistic has one-second cohorts and a two-second minimum. Equal values at that floor cannot establish equal detection/repair speed. Inspect raw request completion times and SWIM events for finer timing; lookup-triggered Chord repair can finish before SWIM confirmation.

Do not combine these results with older Docker/netem or standalone verifier reports: their fault models, workloads and timing differ.

## SWIM diagnostics

Summed local transitions per phase across nodes and repetitions. A single suspicion may be observed at several nodes; these are not unique physical failures. Baseline values are unavailable.

| Phase | SUSPECT transitions | Refutation observations | DEAD transitions | Validated reference writes |
|---|---:|---:|---:|---:|
| healthy | 0 | 0 | 0 | 0 |
| gray_failure | 54 | 67 | 0 | 0 |
| gray_recovered | 0 | 0 | 0 | 0 |
| crash_recovery | 9 | 0 | 9 | 0 |
| post_crash | 0 | 0 | 0 | 0 |
| restart | 0 | 0 | 0 | 0 |
