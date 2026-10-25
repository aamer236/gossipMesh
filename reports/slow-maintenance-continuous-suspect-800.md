# Chord / Chord + standard SWIM comparison

Generated 2026-10-24T22:59:19.274743+00:00. Acceptance passed: True.

Same offered lookup schedule, IDs, Chord timers, gray fault and hard-crash victim. Runs execute sequentially with alternating order.

Values below are medians of per-run results. Raw samples, ranges, counters and SWIM events are in the companion JSON.

| Phase | Variant | Runs | Correct % | p50 ms | p95 ms | p99 ms | Mean hops | Recovery s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| healthy | chord | 3 | 100.000 | 5.198 | 8.723 | 18.797 | 1.750 | — |
| gray_failure | chord | 3 | 100.000 | 6.566 | 108.010 | 108.951 | 1.750 | — |
| gray_recovered | chord | 3 | 100.000 | 5.213 | 8.373 | 14.382 | 1.750 | — |
| crash_recovery | chord | 3 | 100.000 | 4.968 | 7.397 | 8.656 | 1.611 | 0.500 |
| post_crash | chord | 3 | 100.000 | 4.997 | 6.975 | 7.903 | 1.604 | — |
| restart | chord | 3 | 100.000 | 4.984 | 7.810 | 8.441 | 1.750 | — |
| healthy | chord_standard_swim | 3 | 100.000 | 5.111 | 8.147 | 10.491 | 1.750 | — |
| gray_failure | chord_standard_swim | 3 | 100.000 | 6.277 | 108.931 | 119.412 | 1.750 | — |
| gray_recovered | chord_standard_swim | 3 | 100.000 | 5.153 | 7.820 | 11.039 | 1.750 | — |
| crash_recovery | chord_standard_swim | 3 | 100.000 | 4.966 | 9.033 | 22.433 | 1.611 | 0.500 |
| post_crash | chord_standard_swim | 3 | 100.000 | 4.761 | 7.020 | 11.782 | 1.604 | — |
| restart | chord_standard_swim | 3 | 100.000 | 4.959 | 7.685 | 9.648 | 1.750 | — |

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
| chord | 1 | 0.000 | 1.988 | — | 7.083 | 5.057 |
| chord_standard_swim | 1 | 0.000 | 1.051 | — | 7.034 | 4.858 |
| chord_standard_swim | 2 | 0.000 | 1.148 | — | 6.019 | 6.371 |
| chord | 2 | 0.000 | 2.504 | — | 6.085 | 4.314 |
| chord | 3 | 0.000 | 1.555 | — | 6.029 | 3.897 |
| chord_standard_swim | 3 | 0.000 | 1.361 | — | 6.561 | 2.874 |

## Peer traffic and memory

Per-phase medians across runs. Traffic is measured at proxies; Go heap is a whole-process snapshot averaged over active nodes. Neither includes proxy Python memory.

| Phase | Variant | Peer requests | Body bytes | Mean Go heap KiB |
|---|---|---:|---:|---:|
| healthy | chord | 261 | 15429 | 1354.4 |
| gray_failure | chord | 257 | 15053 | 1816.7 |
| gray_recovered | chord | 257 | 15053 | 2069.0 |
| crash_recovery | chord | 335 | 17428 | 2105.3 |
| post_crash | chord | 220 | 11503 | 2349.2 |
| restart | chord | 261 | 15435 | 2009.2 |
| healthy | chord_standard_swim | 338 | 63968 | 2559.9 |
| gray_failure | chord_standard_swim | 471 | 132031 | 2210.0 |
| gray_recovered | chord_standard_swim | 343 | 68141 | 1900.4 |
| crash_recovery | chord_standard_swim | 467 | 92720 | 2184.2 |
| post_crash | chord_standard_swim | 279 | 48187 | 2337.6 |
| restart | chord_standard_swim | 338 | 64086 | 2272.0 |

The recovery floor is twice the configured cohort duration plus any intentional quiet period. Equal values at that floor cannot establish equal detection/repair speed. Read-only routing observations separately record convergence; lookup-triggered Chord repair can finish before SWIM confirmation.

Do not combine these results with older Docker/netem or standalone verifier reports: their fault models, workloads and timing differ.

## SWIM diagnostics

Summed local transitions per phase across nodes and repetitions. A single suspicion may be observed at several nodes; these are not unique physical failures. Baseline values are unavailable.

| Phase | SUSPECT transitions | Refutation observations | DEAD transitions | Validated reference writes |
|---|---:|---:|---:|---:|
| healthy | 0 | 0 | 0 | 0 |
| gray_failure | 52 | 70 | 0 | 0 |
| gray_recovered | 0 | 0 | 0 | 0 |
| crash_recovery | 9 | 0 | 9 | 4 |
| post_crash | 0 | 0 | 0 | 0 |
| restart | 0 | 0 | 0 | 0 |
