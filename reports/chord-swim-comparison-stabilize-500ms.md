# Chord / Chord + standard SWIM comparison

Generated 2026-10-24T22:43:30.889073+00:00. Acceptance passed: True.

Same offered lookup schedule, IDs, Chord timers, gray fault and hard-crash victim. Runs execute sequentially with alternating order.

Values below are medians of per-run results. Raw samples, ranges, counters and SWIM events are in the companion JSON.

| Phase | Variant | Runs | Correct % | p50 ms | p95 ms | p99 ms | Mean hops | Recovery s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| healthy | chord | 3 | 100.000 | 4.786 | 11.346 | 17.017 | 1.750 | — |
| gray_failure | chord | 3 | 100.000 | 6.892 | 109.689 | 116.270 | 1.750 | — |
| gray_recovered | chord | 3 | 100.000 | 5.049 | 12.410 | 23.519 | 1.750 | — |
| crash_recovery | chord | 3 | 100.000 | 4.996 | 9.907 | 15.678 | 1.604 | 2.000 |
| post_crash | chord | 3 | 100.000 | 5.054 | 11.049 | 14.874 | 1.604 | — |
| restart | chord | 3 | 100.000 | 4.965 | 9.612 | 16.561 | 1.750 | — |
| healthy | chord_standard_swim | 3 | 100.000 | 4.819 | 11.900 | 21.059 | 1.750 | — |
| gray_failure | chord_standard_swim | 3 | 100.000 | 7.005 | 110.907 | 115.831 | 1.750 | — |
| gray_recovered | chord_standard_swim | 3 | 100.000 | 4.427 | 11.280 | 25.260 | 1.750 | — |
| crash_recovery | chord_standard_swim | 3 | 100.000 | 4.230 | 10.179 | 15.808 | 1.604 | 2.000 |
| post_crash | chord_standard_swim | 3 | 100.000 | 4.690 | 9.086 | 15.833 | 1.604 | — |
| restart | chord_standard_swim | 3 | 100.000 | 3.902 | 14.313 | 18.271 | 1.750 | — |

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
| healthy | chord | 528 | 26607 | 2352.8 |
| gray_failure | chord | 520 | 25821 | 1985.1 |
| gray_recovered | chord | 525 | 26452 | 1508.4 |
| crash_recovery | chord | 612 | 27510 | 1683.0 |
| post_crash | chord | 407 | 18311 | 1980.3 |
| restart | chord | 527 | 26641 | 2070.9 |
| healthy | chord_standard_swim | 608 | 75564 | 2120.2 |
| gray_failure | chord_standard_swim | 745 | 140197 | 2130.7 |
| gray_recovered | chord_standard_swim | 601 | 77998 | 2300.9 |
| crash_recovery | chord_standard_swim | 745 | 104348 | 2551.6 |
| post_crash | chord_standard_swim | 467 | 55088 | 2474.7 |
| restart | chord_standard_swim | 602 | 75114 | 2177.7 |

The recovery statistic has one-second cohorts and a two-second minimum. Equal values at that floor cannot establish equal detection/repair speed. Inspect raw request completion times and SWIM events for finer timing; lookup-triggered Chord repair can finish before SWIM confirmation.

Do not combine these results with older Docker/netem or standalone verifier reports: their fault models, workloads and timing differ.

## SWIM diagnostics

Summed local transitions per phase across nodes and repetitions. A single suspicion may be observed at several nodes; these are not unique physical failures. Baseline values are unavailable.

| Phase | SUSPECT transitions | Refutation observations | DEAD transitions | Validated reference writes |
|---|---:|---:|---:|---:|
| healthy | 0 | 0 | 0 | 0 |
| gray_failure | 51 | 64 | 0 | 0 |
| gray_recovered | 0 | 0 | 0 | 0 |
| crash_recovery | 9 | 0 | 9 | 0 |
| post_crash | 0 | 0 | 0 | 0 |
| restart | 0 | 0 | 0 | 0 |
