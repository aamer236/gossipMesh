# Chord / Chord + standard SWIM comparison

Generated 2026-10-25T00:17:35.779564+00:00. Acceptance passed: True.

Same offered lookup schedule, IDs, initial Chord timers, gray fault and hard-crash victim. Runs execute sequentially with alternating order. Adaptive maintenance, when enabled, applies only to the SWIM variant; each run's raw config records the policy.

Values below are medians of per-run results. Raw samples, ranges, counters and SWIM events are in the companion JSON.

| Phase | Variant | Runs | Correct % | p50 ms | p95 ms | p99 ms | Mean hops | Recovery s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| healthy | chord | 3 | 100.000 | 5.556 | 8.981 | 10.756 | 1.812 | — |
| gray_failure | chord | 3 | 100.000 | 7.412 | 109.563 | 119.479 | 1.812 | — |
| gray_recovered | chord | 3 | 100.000 | 5.516 | 9.122 | 19.773 | 1.812 | — |
| crash_recovery | chord | 3 | 100.000 | 5.343 | 7.214 | 9.080 | 1.604 | 2.500 |
| post_crash | chord | 3 | 100.000 | 5.335 | 7.550 | 14.432 | 1.604 | — |
| restart | chord | 3 | 100.000 | 5.437 | 8.610 | 10.288 | 1.812 | — |
| healthy | chord_standard_swim | 3 | 100.000 | 5.409 | 8.962 | 11.235 | 1.812 | — |
| gray_failure | chord_standard_swim | 3 | 100.000 | 7.463 | 109.773 | 113.362 | 1.812 | — |
| gray_recovered | chord_standard_swim | 3 | 100.000 | 5.381 | 8.904 | 10.473 | 1.812 | — |
| crash_recovery | chord_standard_swim | 3 | 100.000 | 5.151 | 8.392 | 10.816 | 1.604 | 2.500 |
| post_crash | chord_standard_swim | 3 | 100.000 | 5.239 | 7.698 | 9.161 | 1.604 | — |
| restart | chord_standard_swim | 3 | 100.000 | 5.402 | 8.930 | 10.207 | 1.812 | — |

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
| chord | 1 | 2.000 | 2.611 | — | 9.525 | 3.440 |
| chord_standard_swim | 1 | 2.000 | 1.359 | 1.359 | 21.110 | 4.740 |
| chord_standard_swim | 2 | 2.000 | 1.366 | 1.366 | 8.121 | 4.193 |
| chord | 2 | 2.000 | 4.077 | — | 8.035 | 2.944 |
| chord | 3 | 2.000 | 2.089 | — | 7.534 | 3.546 |
| chord_standard_swim | 3 | 2.000 | 1.362 | 1.362 | 7.091 | 4.091 |

## Peer traffic and memory

Per-phase medians across runs. Traffic is measured at proxies; Go heap is a whole-process snapshot averaged over active nodes. Neither includes proxy Python memory.

| Phase | Variant | Peer requests | Body bytes | Mean Go heap KiB |
|---|---|---:|---:|---:|
| healthy | chord | 534 | 31768 | 1875.1 |
| gray_failure | chord | 528 | 31219 | 2267.5 |
| gray_recovered | chord | 532 | 31597 | 1956.9 |
| crash_recovery | chord | 255 | 13400 | 2078.6 |
| post_crash | chord | 440 | 23006 | 2076.9 |
| restart | chord | 529 | 31371 | 2053.0 |
| healthy | chord_standard_swim | 565 | 84941 | 2038.3 |
| gray_failure | chord_standard_swim | 867 | 166905 | 2303.1 |
| gray_recovered | chord_standard_swim | 594 | 89297 | 2423.8 |
| crash_recovery | chord_standard_swim | 347 | 61202 | 2370.0 |
| post_crash | chord_standard_swim | 453 | 63251 | 2299.3 |
| restart | chord_standard_swim | 572 | 86041 | 2566.0 |

The recovery floor is twice the configured cohort duration plus any intentional quiet period. Equal values at that floor cannot establish equal detection/repair speed. Read-only routing observations separately record convergence; lookup-triggered Chord repair can finish before SWIM confirmation.

Do not combine these results with older Docker/netem or standalone verifier reports: their fault models, workloads and timing differ.

## SWIM diagnostics

Summed local transitions per phase across nodes and repetitions. A single suspicion may be observed at several nodes; these are not unique physical failures. Baseline values are unavailable.

| Phase | SUSPECT transitions | Refutation observations | DEAD transitions | Validated reference writes |
|---|---:|---:|---:|---:|
| healthy | 0 | 0 | 0 | 0 |
| gray_failure | 100 | 127 | 0 | 0 |
| gray_recovered | 0 | 0 | 0 | 0 |
| crash_recovery | 9 | 0 | 9 | 2 |
| post_crash | 0 | 0 | 0 | 0 |
| restart | 0 | 0 | 0 | 0 |
