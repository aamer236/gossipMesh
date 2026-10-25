# Chord / Chord + standard SWIM comparison

Generated 2026-10-24T23:58:24.582806+00:00. Acceptance passed: True.

Same offered lookup schedule, IDs, Chord timers, gray fault and hard-crash victim. Runs execute sequentially with alternating order.

Values below are medians of per-run results. Raw samples, ranges, counters and SWIM events are in the companion JSON.

| Phase | Variant | Runs | Correct % | p50 ms | p95 ms | p99 ms | Mean hops | Recovery s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| healthy | chord | 1 | 100.000 | 6.560 | 16.120 | 39.259 | 2.250 | — |
| gray_failure | chord | 1 | 100.000 | 7.746 | 112.732 | 143.257 | 2.250 | — |
| gray_recovered | chord | 1 | 100.000 | 7.014 | 36.032 | 74.183 | 2.250 | — |
| crash_recovery | chord | 1 | 100.000 | 6.792 | 11.823 | 38.533 | 2.211 | 2.500 |
| post_crash | chord | 1 | 100.000 | 6.559 | 12.360 | 29.156 | 2.188 | — |
| restart | chord | 1 | 100.000 | 6.896 | 13.572 | 38.309 | 2.250 | — |
| healthy | chord_standard_swim | 1 | 100.000 | 6.421 | 14.630 | 41.855 | 2.250 | — |
| gray_failure | chord_standard_swim | 1 | 100.000 | 8.443 | 110.020 | 117.194 | 2.250 | — |
| gray_recovered | chord_standard_swim | 1 | 100.000 | 6.766 | 17.236 | 37.039 | 2.250 | — |
| crash_recovery | chord_standard_swim | 1 | 100.000 | 6.749 | 14.168 | 37.398 | 2.188 | 2.500 |
| post_crash | chord_standard_swim | 1 | 100.000 | 6.642 | 32.604 | 68.876 | 2.188 | — |
| restart | chord_standard_swim | 1 | 100.000 | 7.155 | 16.649 | 52.641 | 2.250 | — |

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
| chord | 1 | 2.000 | — | — | 16.542 | 10.038 |
| chord_standard_swim | 1 | 2.000 | 1.435 | 1.435 | 13.097 | 6.545 |

## Peer traffic and memory

Per-phase medians across runs. Traffic is measured at proxies; Go heap is a whole-process snapshot averaged over active nodes. Neither includes proxy Python memory.

| Phase | Variant | Peer requests | Body bytes | Mean Go heap KiB |
|---|---|---:|---:|---:|
| healthy | chord | 472 | 33064 | 2377.4 |
| gray_failure | chord | 471 | 32926 | 1929.5 |
| gray_recovered | chord | 480 | 33985 | 1300.6 |
| crash_recovery | chord | 538 | 36477 | 1551.0 |
| post_crash | chord | 442 | 30411 | 1426.4 |
| restart | chord | 472 | 33056 | 1691.2 |
| healthy | chord_standard_swim | 646 | 99918 | 2255.4 |
| gray_failure | chord_standard_swim | 731 | 142811 | 2197.1 |
| gray_recovered | chord_standard_swim | 644 | 104560 | 2240.0 |
| crash_recovery | chord_standard_swim | 778 | 142944 | 2437.3 |
| post_crash | chord_standard_swim | 599 | 94344 | 2576.9 |
| restart | chord_standard_swim | 638 | 96962 | 2218.3 |

The recovery floor is twice the configured cohort duration plus any intentional quiet period. Equal values at that floor cannot establish equal detection/repair speed. Read-only routing observations separately record convergence; lookup-triggered Chord repair can finish before SWIM confirmation.

Do not combine these results with older Docker/netem or standalone verifier reports: their fault models, workloads and timing differ.

## SWIM diagnostics

Summed local transitions per phase across nodes and repetitions. A single suspicion may be observed at several nodes; these are not unique physical failures. Baseline values are unavailable.

| Phase | SUSPECT transitions | Refutation observations | DEAD transitions | Validated reference writes |
|---|---:|---:|---:|---:|
| healthy | 0 | 0 | 0 | 0 |
| gray_failure | 28 | 32 | 0 | 0 |
| gray_recovered | 0 | 0 | 0 | 0 |
| crash_recovery | 7 | 0 | 7 | 5 |
| post_crash | 1 | 0 | 0 | 0 |
| restart | 0 | 0 | 0 | 0 |
