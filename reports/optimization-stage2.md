# Chord / Chord + standard SWIM comparison

Generated 2026-10-24T23:49:53.918562+00:00. Acceptance passed: True.

Same offered lookup schedule, IDs, Chord timers, gray fault and hard-crash victim. Runs execute sequentially with alternating order.

Values below are medians of per-run results. Raw samples, ranges, counters and SWIM events are in the companion JSON.

| Phase | Variant | Runs | Correct % | p50 ms | p95 ms | p99 ms | Mean hops | Recovery s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| healthy | chord | 3 | 100.000 | 5.358 | 9.522 | 13.190 | 1.750 | — |
| gray_failure | chord | 3 | 100.000 | 7.661 | 109.399 | 116.279 | 1.750 | — |
| gray_recovered | chord | 3 | 100.000 | 5.444 | 8.788 | 11.389 | 1.750 | — |
| crash_recovery | chord | 3 | 100.000 | 5.197 | 8.604 | 10.712 | 1.604 | 2.500 |
| post_crash | chord | 3 | 100.000 | 5.357 | 7.889 | 11.894 | 1.604 | — |
| restart | chord | 3 | 100.000 | 5.450 | 9.052 | 11.001 | 1.750 | — |
| healthy | chord_standard_swim | 3 | 100.000 | 5.530 | 11.883 | 32.906 | 1.750 | — |
| gray_failure | chord_standard_swim | 3 | 100.000 | 8.576 | 110.034 | 116.431 | 1.750 | — |
| gray_recovered | chord_standard_swim | 3 | 100.000 | 5.300 | 8.875 | 16.104 | 1.750 | — |
| crash_recovery | chord_standard_swim | 3 | 100.000 | 5.080 | 9.563 | 18.225 | 1.604 | 2.500 |
| post_crash | chord_standard_swim | 3 | 100.000 | 5.058 | 7.682 | 15.531 | 1.604 | — |
| restart | chord_standard_swim | 3 | 100.000 | 5.570 | 9.617 | 23.891 | 1.750 | — |

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
| chord | 1 | 2.000 | 2.506 | — | 8.529 | 2.831 |
| chord_standard_swim | 1 | 2.000 | 1.064 | 1.064 | 6.041 | 5.547 |
| chord_standard_swim | 2 | 2.000 | 1.997 | 1.997 | 6.043 | 7.063 |
| chord | 2 | 2.000 | 3.038 | — | 7.029 | 5.364 |
| chord | 3 | 2.000 | 3.540 | — | 6.035 | 3.911 |
| chord_standard_swim | 3 | 2.000 | 1.268 | 1.268 | 7.044 | 3.349 |

## Peer traffic and memory

Per-phase medians across runs. Traffic is measured at proxies; Go heap is a whole-process snapshot averaged over active nodes. Neither includes proxy Python memory.

| Phase | Variant | Peer requests | Body bytes | Mean Go heap KiB |
|---|---|---:|---:|---:|
| healthy | chord | 258 | 15146 | 1570.9 |
| gray_failure | chord | 255 | 14857 | 1700.6 |
| gray_recovered | chord | 257 | 15051 | 2065.9 |
| crash_recovery | chord | 256 | 13498 | 2385.7 |
| post_crash | chord | 219 | 11407 | 1742.2 |
| restart | chord | 259 | 15247 | 2206.4 |
| healthy | chord_standard_swim | 336 | 45722 | 2373.7 |
| gray_failure | chord_standard_swim | 437 | 84389 | 2221.9 |
| gray_recovered | chord_standard_swim | 334 | 47264 | 2073.8 |
| crash_recovery | chord_standard_swim | 371 | 62439 | 2250.8 |
| post_crash | chord_standard_swim | 279 | 34626 | 2341.5 |
| restart | chord_standard_swim | 338 | 46022 | 2226.4 |

The recovery floor is twice the configured cohort duration plus any intentional quiet period. Equal values at that floor cannot establish equal detection/repair speed. Read-only routing observations separately record convergence; lookup-triggered Chord repair can finish before SWIM confirmation.

Do not combine these results with older Docker/netem or standalone verifier reports: their fault models, workloads and timing differ.

## SWIM diagnostics

Summed local transitions per phase across nodes and repetitions. A single suspicion may be observed at several nodes; these are not unique physical failures. Baseline values are unavailable.

| Phase | SUSPECT transitions | Refutation observations | DEAD transitions | Validated reference writes |
|---|---:|---:|---:|---:|
| healthy | 0 | 0 | 0 | 0 |
| gray_failure | 51 | 68 | 0 | 0 |
| gray_recovered | 0 | 0 | 0 | 0 |
| crash_recovery | 9 | 0 | 9 | 2 |
| post_crash | 0 | 0 | 0 | 0 |
| restart | 0 | 0 | 0 | 0 |
