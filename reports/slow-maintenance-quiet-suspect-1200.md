# Chord / Chord + standard SWIM comparison

Generated 2026-10-24T23:03:07.931263+00:00. Acceptance passed: True.

Same offered lookup schedule, IDs, Chord timers, gray fault and hard-crash victim. Runs execute sequentially with alternating order.

Values below are medians of per-run results. Raw samples, ranges, counters and SWIM events are in the companion JSON.

| Phase | Variant | Runs | Correct % | p50 ms | p95 ms | p99 ms | Mean hops | Recovery s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| healthy | chord | 3 | 100.000 | 4.767 | 7.807 | 10.266 | 1.750 | — |
| gray_failure | chord | 3 | 100.000 | 6.209 | 108.745 | 114.801 | 1.750 | — |
| gray_recovered | chord | 3 | 100.000 | 5.177 | 7.771 | 13.512 | 1.750 | — |
| crash_recovery | chord | 3 | 100.000 | 4.915 | 7.437 | 9.356 | 1.604 | 2.500 |
| post_crash | chord | 3 | 100.000 | 4.962 | 7.377 | 11.715 | 1.604 | — |
| restart | chord | 3 | 100.000 | 4.948 | 7.639 | 8.847 | 1.750 | — |
| healthy | chord_standard_swim | 3 | 100.000 | 4.826 | 7.587 | 10.120 | 1.750 | — |
| gray_failure | chord_standard_swim | 3 | 100.000 | 7.094 | 108.242 | 115.806 | 1.750 | — |
| gray_recovered | chord_standard_swim | 3 | 100.000 | 5.015 | 8.977 | 26.683 | 1.750 | — |
| crash_recovery | chord_standard_swim | 3 | 100.000 | 4.720 | 7.848 | 13.578 | 1.604 | 2.500 |
| post_crash | chord_standard_swim | 3 | 100.000 | 3.723 | 6.295 | 7.694 | 1.604 | — |
| restart | chord_standard_swim | 3 | 100.000 | 4.486 | 7.664 | 12.454 | 1.750 | — |

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
| chord | 1 | 2.000 | 1.979 | 1.979 | 6.031 | 3.350 |
| chord_standard_swim | 1 | 2.000 | 1.038 | 1.038 | 7.034 | 5.067 |
| chord_standard_swim | 2 | 2.000 | 1.453 | 1.453 | 5.044 | 2.875 |
| chord | 2 | 2.000 | 1.561 | 1.561 | 6.531 | 5.416 |
| chord | 3 | 2.000 | 2.078 | — | 8.024 | 5.888 |
| chord_standard_swim | 3 | 2.000 | 1.562 | 1.562 | 8.524 | 2.430 |

## Peer traffic and memory

Per-phase medians across runs. Traffic is measured at proxies; Go heap is a whole-process snapshot averaged over active nodes. Neither includes proxy Python memory.

| Phase | Variant | Peer requests | Body bytes | Mean Go heap KiB |
|---|---|---:|---:|---:|
| healthy | chord | 257 | 15051 | 1514.5 |
| gray_failure | chord | 258 | 15146 | 2458.8 |
| gray_recovered | chord | 260 | 15336 | 2211.0 |
| crash_recovery | chord | 253 | 13183 | 2195.8 |
| post_crash | chord | 221 | 11598 | 1752.9 |
| restart | chord | 261 | 15433 | 2309.3 |
| healthy | chord_standard_swim | 337 | 63873 | 2027.6 |
| gray_failure | chord_standard_swim | 469 | 129084 | 2195.3 |
| gray_recovered | chord_standard_swim | 340 | 66527 | 2444.5 |
| crash_recovery | chord_standard_swim | 388 | 92064 | 2233.2 |
| post_crash | chord_standard_swim | 279 | 48192 | 2197.5 |
| restart | chord_standard_swim | 339 | 64173 | 2142.0 |

The recovery floor is twice the configured cohort duration plus any intentional quiet period. Equal values at that floor cannot establish equal detection/repair speed. Read-only routing observations separately record convergence; lookup-triggered Chord repair can finish before SWIM confirmation.

Do not combine these results with older Docker/netem or standalone verifier reports: their fault models, workloads and timing differ.

## SWIM diagnostics

Summed local transitions per phase across nodes and repetitions. A single suspicion may be observed at several nodes; these are not unique physical failures. Baseline values are unavailable.

| Phase | SUSPECT transitions | Refutation observations | DEAD transitions | Validated reference writes |
|---|---:|---:|---:|---:|
| healthy | 0 | 0 | 0 | 0 |
| gray_failure | 48 | 63 | 0 | 0 |
| gray_recovered | 0 | 0 | 0 | 0 |
| crash_recovery | 9 | 0 | 9 | 1 |
| post_crash | 0 | 0 | 0 | 0 |
| restart | 0 | 0 | 0 | 0 |
