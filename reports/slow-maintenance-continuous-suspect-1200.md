# Chord / Chord + standard SWIM comparison

Generated 2026-10-24T22:55:20.716406+00:00. Acceptance passed: True.

Same offered lookup schedule, IDs, Chord timers, gray fault and hard-crash victim. Runs execute sequentially with alternating order.

Values below are medians of per-run results. Raw samples, ranges, counters and SWIM events are in the companion JSON.

| Phase | Variant | Runs | Correct % | p50 ms | p95 ms | p99 ms | Mean hops | Recovery s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| healthy | chord | 3 | 100.000 | 5.224 | 7.677 | 9.430 | 1.750 | — |
| gray_failure | chord | 3 | 100.000 | 6.149 | 108.206 | 111.892 | 1.750 | — |
| gray_recovered | chord | 3 | 100.000 | 5.171 | 7.599 | 8.589 | 1.750 | — |
| crash_recovery | chord | 3 | 100.000 | 5.041 | 10.326 | 23.206 | 1.639 | 0.500 |
| post_crash | chord | 3 | 100.000 | 4.984 | 8.079 | 10.995 | 1.604 | — |
| restart | chord | 3 | 100.000 | 5.192 | 8.041 | 8.774 | 1.750 | — |
| healthy | chord_standard_swim | 3 | 100.000 | 5.187 | 8.179 | 18.111 | 1.750 | — |
| gray_failure | chord_standard_swim | 3 | 100.000 | 7.542 | 109.099 | 123.572 | 1.750 | — |
| gray_recovered | chord_standard_swim | 3 | 100.000 | 5.211 | 8.772 | 20.740 | 1.750 | — |
| crash_recovery | chord_standard_swim | 3 | 100.000 | 5.063 | 7.209 | 8.434 | 1.604 | 0.500 |
| post_crash | chord_standard_swim | 3 | 100.000 | 4.941 | 7.306 | 9.221 | 1.604 | — |
| restart | chord_standard_swim | 3 | 100.000 | 4.998 | 7.930 | 10.000 | 1.750 | — |

Fault: 100% of incoming peer RPCs to one live victim delayed before forwarding; client lookups/management bypass proxy; no packet loss; after healing kill same victim.

Recovery: two consecutive 250ms scheduled cohorts >=99% oracle-correct per survivor; completion time and intentional quiet delay included; null means not observed within fixed window.

Traffic: destination proxy request attempts and JSON body bytes, including SWIM; no HTTP/TCP/IP overhead; proxy errors include client disconnects.

Gray-failure oracle includes the delayed live node. Only the hard-crash phase removes that node from the oracle. Gray recovery is checked before the crash; restart uses the same identity/address and persisted SWIM incarnation.

SWIM events/metrics have no baseline equivalent and are null for Chord. Whole-process memory snapshots are provided, not SWIM-only memory. Lookup latency includes failed requests; mean hops includes correct requests only.

- Local proxy overhead affects both variants; not equivalent to netem packet delay.
- Small-ring sampled lookup results do not prove arbitrary-ring or partition correctness.
- Sequential alternating paired runs; no statistical significance claim.
- Null metrics are unsupported/unobserved, never inferred zero.

## Background routing observation

State is sampled through read-only /state requests about every 100ms. Routing convergence requires two consecutive samples with correct neighbors, successor lists and every finger. This observer never supplies routes or issues lookup RPCs. Quiet convergence counts only observations before the first scheduled client lookup; a blank value means it was not observed in that window.

| Variant | Trial | Lookup delay s | Routing converged s | During quiet window s | Initial convergence s | Rejoin convergence s |
|---|---:|---:|---:|---:|---:|---:|
| chord | 1 | 0.000 | 4.602 | — | 7.522 | 7.569 |
| chord_standard_swim | 1 | 0.000 | 1.787 | — | 6.034 | 5.742 |
| chord_standard_swim | 2 | 0.000 | 1.755 | — | 6.528 | 8.046 |
| chord | 2 | 0.000 | 3.030 | — | 6.040 | 4.857 |
| chord | 3 | 0.000 | 3.548 | — | 6.520 | 3.432 |
| chord_standard_swim | 3 | 0.000 | 1.777 | — | 8.093 | 4.810 |

## Peer traffic and memory

Per-phase medians across runs. Traffic is measured at proxies; Go heap is a whole-process snapshot averaged over active nodes. Neither includes proxy Python memory.

| Phase | Variant | Peer requests | Body bytes | Mean Go heap KiB |
|---|---|---:|---:|---:|
| healthy | chord | 262 | 15532 | 1643.7 |
| gray_failure | chord | 257 | 15034 | 1335.3 |
| gray_recovered | chord | 260 | 15340 | 2364.3 |
| crash_recovery | chord | 344 | 17881 | 1178.2 |
| post_crash | chord | 219 | 11407 | 2195.9 |
| restart | chord | 258 | 15161 | 1815.5 |
| healthy | chord_standard_swim | 337 | 63869 | 2087.3 |
| gray_failure | chord_standard_swim | 467 | 129297 | 2211.5 |
| gray_recovered | chord_standard_swim | 338 | 67173 | 2261.9 |
| crash_recovery | chord_standard_swim | 472 | 97129 | 1970.5 |
| post_crash | chord_standard_swim | 277 | 47997 | 2099.4 |
| restart | chord_standard_swim | 339 | 64179 | 2103.0 |

The recovery floor is twice the configured cohort duration plus any intentional quiet period. Equal values at that floor cannot establish equal detection/repair speed. Read-only routing observations separately record convergence; lookup-triggered Chord repair can finish before SWIM confirmation.

Do not combine these results with older Docker/netem or standalone verifier reports: their fault models, workloads and timing differ.

## SWIM diagnostics

Summed local transitions per phase across nodes and repetitions. A single suspicion may be observed at several nodes; these are not unique physical failures. Baseline values are unavailable.

| Phase | SUSPECT transitions | Refutation observations | DEAD transitions | Validated reference writes |
|---|---:|---:|---:|---:|
| healthy | 0 | 0 | 0 | 0 |
| gray_failure | 54 | 66 | 0 | 0 |
| gray_recovered | 0 | 0 | 0 | 0 |
| crash_recovery | 9 | 0 | 9 | 3 |
| post_crash | 0 | 0 | 0 | 0 |
| restart | 0 | 0 | 0 | 0 |
