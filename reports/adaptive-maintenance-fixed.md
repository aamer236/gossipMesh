# Chord / Chord + standard SWIM comparison

Generated 2026-10-25T00:11:38.524715+00:00. Acceptance passed: True.

Same offered lookup schedule, IDs, initial Chord timers, gray fault and hard-crash victim. Runs execute sequentially with alternating order. Adaptive maintenance, when enabled, applies only to the SWIM variant; each run's raw config records the policy.

Values below are medians of per-run results. Raw samples, ranges, counters and SWIM events are in the companion JSON.

| Phase | Variant | Runs | Correct % | p50 ms | p95 ms | p99 ms | Mean hops | Recovery s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| healthy | chord | 3 | 100.000 | 5.434 | 8.331 | 9.987 | 1.812 | — |
| gray_failure | chord | 3 | 100.000 | 7.580 | 109.571 | 120.682 | 1.812 | — |
| gray_recovered | chord | 3 | 100.000 | 5.497 | 9.023 | 12.577 | 1.812 | — |
| crash_recovery | chord | 3 | 100.000 | 5.315 | 8.188 | 9.865 | 1.604 | 2.500 |
| post_crash | chord | 3 | 100.000 | 5.300 | 8.736 | 11.716 | 1.604 | — |
| restart | chord | 3 | 100.000 | 5.476 | 8.590 | 9.649 | 1.812 | — |
| healthy | chord_standard_swim | 3 | 100.000 | 5.427 | 12.599 | 33.670 | 1.812 | — |
| gray_failure | chord_standard_swim | 3 | 100.000 | 8.434 | 110.667 | 114.382 | 1.812 | — |
| gray_recovered | chord_standard_swim | 3 | 100.000 | 5.443 | 10.563 | 29.342 | 1.812 | — |
| crash_recovery | chord_standard_swim | 3 | 100.000 | 5.267 | 8.756 | 16.314 | 1.604 | 2.500 |
| post_crash | chord_standard_swim | 3 | 100.000 | 5.172 | 7.649 | 9.162 | 1.604 | — |
| restart | chord_standard_swim | 3 | 100.000 | 5.356 | 8.641 | 10.417 | 1.812 | — |

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
| chord | 1 | 2.000 | 2.109 | — | 12.032 | 3.050 |
| chord_standard_swim | 1 | 2.000 | 1.364 | 1.364 | 7.051 | 3.245 |
| chord_standard_swim | 2 | 2.000 | 1.360 | 1.360 | 6.043 | 5.415 |
| chord | 2 | 2.000 | 1.577 | 1.577 | 7.533 | 3.924 |
| chord | 3 | 2.000 | 3.560 | — | 7.528 | 3.936 |
| chord_standard_swim | 3 | 2.000 | 1.360 | 1.360 | 8.549 | 2.949 |

## Peer traffic and memory

Per-phase medians across runs. Traffic is measured at proxies; Go heap is a whole-process snapshot averaged over active nodes. Neither includes proxy Python memory.

| Phase | Variant | Peer requests | Body bytes | Mean Go heap KiB |
|---|---|---:|---:|---:|
| healthy | chord | 527 | 31109 | 1386.2 |
| gray_failure | chord | 532 | 31561 | 2202.1 |
| gray_recovered | chord | 529 | 31310 | 1910.2 |
| crash_recovery | chord | 259 | 13486 | 1824.5 |
| post_crash | chord | 438 | 22814 | 2219.0 |
| restart | chord | 535 | 31861 | 2492.0 |
| healthy | chord_standard_swim | 681 | 92869 | 1848.3 |
| gray_failure | chord_standard_swim | 876 | 166589 | 2521.9 |
| gray_recovered | chord_standard_swim | 680 | 94924 | 2367.2 |
| crash_recovery | chord_standard_swim | 362 | 62323 | 2406.5 |
| post_crash | chord_standard_swim | 549 | 69103 | 2137.5 |
| restart | chord_standard_swim | 681 | 93074 | 2352.9 |

The recovery floor is twice the configured cohort duration plus any intentional quiet period. Equal values at that floor cannot establish equal detection/repair speed. Read-only routing observations separately record convergence; lookup-triggered Chord repair can finish before SWIM confirmation.

Do not combine these results with older Docker/netem or standalone verifier reports: their fault models, workloads and timing differ.

## SWIM diagnostics

Summed local transitions per phase across nodes and repetitions. A single suspicion may be observed at several nodes; these are not unique physical failures. Baseline values are unavailable.

| Phase | SUSPECT transitions | Refutation observations | DEAD transitions | Validated reference writes |
|---|---:|---:|---:|---:|
| healthy | 0 | 0 | 0 | 0 |
| gray_failure | 97 | 125 | 0 | 0 |
| gray_recovered | 0 | 0 | 0 | 0 |
| crash_recovery | 9 | 0 | 9 | 4 |
| post_crash | 0 | 0 | 0 | 0 |
| restart | 0 | 0 | 0 | 0 |
