# Chord / Chord + standard SWIM comparison

Generated 2026-10-24T23:45:53.742507+00:00. Acceptance passed: True.

Same offered lookup schedule, IDs, Chord timers, gray fault and hard-crash victim. Runs execute sequentially with alternating order.

Values below are medians of per-run results. Raw samples, ranges, counters and SWIM events are in the companion JSON.

| Phase | Variant | Runs | Correct % | p50 ms | p95 ms | p99 ms | Mean hops | Recovery s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| healthy | chord | 3 | 100.000 | 5.393 | 8.657 | 17.940 | 1.750 | — |
| gray_failure | chord | 3 | 100.000 | 7.309 | 109.320 | 119.606 | 1.750 | — |
| gray_recovered | chord | 3 | 100.000 | 5.438 | 9.171 | 27.987 | 1.750 | — |
| crash_recovery | chord | 3 | 100.000 | 5.155 | 7.915 | 13.452 | 1.604 | 2.500 |
| post_crash | chord | 3 | 100.000 | 5.331 | 8.218 | 9.516 | 1.604 | — |
| restart | chord | 3 | 100.000 | 5.436 | 9.101 | 11.953 | 1.750 | — |
| healthy | chord_standard_swim | 3 | 100.000 | 5.330 | 12.154 | 33.688 | 1.750 | — |
| gray_failure | chord_standard_swim | 3 | 100.000 | 7.526 | 110.928 | 120.839 | 1.750 | — |
| gray_recovered | chord_standard_swim | 3 | 100.000 | 5.429 | 9.026 | 21.007 | 1.750 | — |
| crash_recovery | chord_standard_swim | 3 | 100.000 | 5.279 | 8.740 | 10.764 | 1.604 | 2.500 |
| post_crash | chord_standard_swim | 3 | 100.000 | 5.288 | 8.742 | 28.669 | 1.604 | — |
| restart | chord_standard_swim | 3 | 100.000 | 5.248 | 8.828 | 11.638 | 1.750 | — |

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
| chord | 1 | 2.000 | 0.957 | 0.957 | 7.043 | 4.838 |
| chord_standard_swim | 1 | 2.000 | 1.580 | 1.580 | 6.531 | 3.353 |
| chord_standard_swim | 2 | 2.000 | 2.006 | — | 7.056 | 8.344 |
| chord | 2 | 2.000 | 1.980 | 1.980 | 8.033 | 3.870 |
| chord | 3 | 2.000 | 3.030 | — | 7.534 | 6.362 |
| chord_standard_swim | 3 | 2.000 | 1.890 | 1.890 | 7.535 | 3.774 |

## Peer traffic and memory

Per-phase medians across runs. Traffic is measured at proxies; Go heap is a whole-process snapshot averaged over active nodes. Neither includes proxy Python memory.

| Phase | Variant | Peer requests | Body bytes | Mean Go heap KiB |
|---|---|---:|---:|---:|
| healthy | chord | 258 | 15150 | 2066.3 |
| gray_failure | chord | 259 | 15239 | 1781.0 |
| gray_recovered | chord | 260 | 15340 | 2435.1 |
| crash_recovery | chord | 254 | 13210 | 1568.9 |
| post_crash | chord | 219 | 11407 | 1934.8 |
| restart | chord | 260 | 15338 | 1683.1 |
| healthy | chord_standard_swim | 338 | 63958 | 2318.3 |
| gray_failure | chord_standard_swim | 438 | 127522 | 1957.7 |
| gray_recovered | chord_standard_swim | 336 | 67082 | 2284.9 |
| crash_recovery | chord_standard_swim | 383 | 88035 | 2047.6 |
| post_crash | chord_standard_swim | 279 | 48184 | 2069.5 |
| restart | chord_standard_swim | 338 | 64092 | 2229.5 |

The recovery floor is twice the configured cohort duration plus any intentional quiet period. Equal values at that floor cannot establish equal detection/repair speed. Read-only routing observations separately record convergence; lookup-triggered Chord repair can finish before SWIM confirmation.

Do not combine these results with older Docker/netem or standalone verifier reports: their fault models, workloads and timing differ.

## SWIM diagnostics

Summed local transitions per phase across nodes and repetitions. A single suspicion may be observed at several nodes; these are not unique physical failures. Baseline values are unavailable.

| Phase | SUSPECT transitions | Refutation observations | DEAD transitions | Validated reference writes |
|---|---:|---:|---:|---:|
| healthy | 0 | 0 | 0 | 0 |
| gray_failure | 50 | 64 | 0 | 0 |
| gray_recovered | 0 | 0 | 0 | 0 |
| crash_recovery | 9 | 0 | 9 | 5 |
| post_crash | 0 | 0 | 0 | 0 |
| restart | 0 | 0 | 0 | 0 |
