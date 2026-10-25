# Chord / Chord + standard SWIM comparison

Generated 2026-10-24T23:06:57.599067+00:00. Acceptance passed: True.

Same offered lookup schedule, IDs, Chord timers, gray fault and hard-crash victim. Runs execute sequentially with alternating order.

Values below are medians of per-run results. Raw samples, ranges, counters and SWIM events are in the companion JSON.

| Phase | Variant | Runs | Correct % | p50 ms | p95 ms | p99 ms | Mean hops | Recovery s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| healthy | chord | 3 | 100.000 | 4.680 | 7.598 | 8.963 | 1.750 | — |
| gray_failure | chord | 3 | 100.000 | 5.787 | 107.967 | 112.819 | 1.750 | — |
| gray_recovered | chord | 3 | 100.000 | 5.109 | 7.871 | 11.590 | 1.750 | — |
| crash_recovery | chord | 3 | 100.000 | 4.816 | 6.899 | 8.866 | 1.604 | 2.500 |
| post_crash | chord | 3 | 100.000 | 4.072 | 6.927 | 7.810 | 1.604 | — |
| restart | chord | 3 | 100.000 | 5.030 | 7.424 | 8.618 | 1.750 | — |
| healthy | chord_standard_swim | 3 | 100.000 | 4.882 | 7.828 | 9.434 | 1.750 | — |
| gray_failure | chord_standard_swim | 3 | 100.000 | 6.780 | 108.632 | 113.982 | 1.750 | — |
| gray_recovered | chord_standard_swim | 3 | 100.000 | 4.895 | 8.246 | 11.666 | 1.750 | — |
| crash_recovery | chord_standard_swim | 3 | 100.000 | 4.782 | 7.647 | 15.185 | 1.604 | 2.500 |
| post_crash | chord_standard_swim | 3 | 100.000 | 4.697 | 7.421 | 9.612 | 1.604 | — |
| restart | chord_standard_swim | 3 | 100.000 | 5.129 | 8.445 | 21.653 | 1.750 | — |

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
| chord | 1 | 2.000 | 3.025 | — | 6.034 | 9.372 |
| chord_standard_swim | 1 | 2.000 | 1.353 | 1.353 | 9.035 | 3.775 |
| chord_standard_swim | 2 | 2.000 | 1.353 | 1.353 | 5.554 | 3.363 |
| chord | 2 | 2.000 | 2.080 | — | 6.513 | 2.553 |
| chord | 3 | 2.000 | 3.006 | — | 8.033 | 4.525 |
| chord_standard_swim | 3 | 2.000 | 1.141 | 1.141 | 6.526 | 3.429 |

## Peer traffic and memory

Per-phase medians across runs. Traffic is measured at proxies; Go heap is a whole-process snapshot averaged over active nodes. Neither includes proxy Python memory.

| Phase | Variant | Peer requests | Body bytes | Mean Go heap KiB |
|---|---|---:|---:|---:|
| healthy | chord | 260 | 15338 | 1631.4 |
| gray_failure | chord | 259 | 15241 | 2055.5 |
| gray_recovered | chord | 260 | 15334 | 2157.8 |
| crash_recovery | chord | 253 | 13212 | 1707.8 |
| post_crash | chord | 220 | 11501 | 1947.6 |
| restart | chord | 257 | 15053 | 1836.2 |
| healthy | chord_standard_swim | 343 | 64441 | 2122.8 |
| gray_failure | chord_standard_swim | 459 | 127188 | 2436.3 |
| gray_recovered | chord_standard_swim | 336 | 67225 | 2078.9 |
| crash_recovery | chord_standard_swim | 383 | 85839 | 2076.2 |
| post_crash | chord_standard_swim | 280 | 48282 | 2492.5 |
| restart | chord_standard_swim | 341 | 64371 | 2086.8 |

The recovery floor is twice the configured cohort duration plus any intentional quiet period. Equal values at that floor cannot establish equal detection/repair speed. Read-only routing observations separately record convergence; lookup-triggered Chord repair can finish before SWIM confirmation.

Do not combine these results with older Docker/netem or standalone verifier reports: their fault models, workloads and timing differ.

## SWIM diagnostics

Summed local transitions per phase across nodes and repetitions. A single suspicion may be observed at several nodes; these are not unique physical failures. Baseline values are unavailable.

| Phase | SUSPECT transitions | Refutation observations | DEAD transitions | Validated reference writes |
|---|---:|---:|---:|---:|
| healthy | 0 | 0 | 0 | 0 |
| gray_failure | 51 | 66 | 0 | 0 |
| gray_recovered | 0 | 0 | 0 | 0 |
| crash_recovery | 9 | 0 | 9 | 2 |
| post_crash | 0 | 0 | 0 | 0 |
| restart | 0 | 0 | 0 | 0 |
