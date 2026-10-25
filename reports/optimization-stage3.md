# Chord / Chord + standard SWIM comparison

Generated 2026-10-24T23:53:46.478058+00:00. Acceptance passed: True.

Same offered lookup schedule, IDs, Chord timers, gray fault and hard-crash victim. Runs execute sequentially with alternating order.

Values below are medians of per-run results. Raw samples, ranges, counters and SWIM events are in the companion JSON.

| Phase | Variant | Runs | Correct % | p50 ms | p95 ms | p99 ms | Mean hops | Recovery s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| healthy | chord | 3 | 100.000 | 5.394 | 8.597 | 9.319 | 1.750 | — |
| gray_failure | chord | 3 | 100.000 | 7.926 | 109.326 | 123.431 | 1.750 | — |
| gray_recovered | chord | 3 | 100.000 | 5.384 | 10.167 | 23.792 | 1.750 | — |
| crash_recovery | chord | 3 | 100.000 | 5.136 | 7.994 | 9.625 | 1.604 | 2.500 |
| post_crash | chord | 3 | 100.000 | 5.244 | 7.980 | 9.495 | 1.604 | — |
| restart | chord | 3 | 100.000 | 5.372 | 8.495 | 9.803 | 1.750 | — |
| healthy | chord_standard_swim | 3 | 100.000 | 5.369 | 10.487 | 36.284 | 1.750 | — |
| gray_failure | chord_standard_swim | 3 | 100.000 | 7.950 | 110.541 | 119.178 | 1.750 | — |
| gray_recovered | chord_standard_swim | 3 | 100.000 | 5.258 | 8.402 | 10.220 | 1.750 | — |
| crash_recovery | chord_standard_swim | 3 | 100.000 | 5.266 | 7.664 | 9.408 | 1.604 | 2.500 |
| post_crash | chord_standard_swim | 3 | 100.000 | 5.149 | 8.535 | 18.274 | 1.604 | — |
| restart | chord_standard_swim | 3 | 100.000 | 5.432 | 10.469 | 23.352 | 1.750 | — |

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
| chord | 1 | 2.000 | 1.553 | 1.553 | 7.034 | 7.856 |
| chord_standard_swim | 1 | 2.000 | 1.568 | 1.568 | 6.539 | 7.356 |
| chord_standard_swim | 2 | 2.000 | 1.361 | 1.361 | 6.541 | 5.184 |
| chord | 2 | 2.000 | 1.582 | 1.582 | 8.529 | 3.533 |
| chord | 3 | 2.000 | 1.982 | 1.982 | 7.530 | 5.369 |
| chord_standard_swim | 3 | 2.000 | 1.998 | 1.998 | 6.042 | 4.565 |

## Peer traffic and memory

Per-phase medians across runs. Traffic is measured at proxies; Go heap is a whole-process snapshot averaged over active nodes. Neither includes proxy Python memory.

| Phase | Variant | Peer requests | Body bytes | Mean Go heap KiB |
|---|---|---:|---:|---:|
| healthy | chord | 259 | 15239 | 2076.9 |
| gray_failure | chord | 257 | 15049 | 1638.9 |
| gray_recovered | chord | 257 | 15051 | 1957.4 |
| crash_recovery | chord | 257 | 13266 | 1565.4 |
| post_crash | chord | 220 | 11501 | 2297.4 |
| restart | chord | 262 | 15530 | 1877.3 |
| healthy | chord_standard_swim | 331 | 45707 | 2224.4 |
| gray_failure | chord_standard_swim | 434 | 83494 | 2178.4 |
| gray_recovered | chord_standard_swim | 334 | 47754 | 1957.9 |
| crash_recovery | chord_standard_swim | 369 | 63848 | 1890.6 |
| post_crash | chord_standard_swim | 271 | 34334 | 2268.5 |
| restart | chord_standard_swim | 332 | 45971 | 2331.6 |

The recovery floor is twice the configured cohort duration plus any intentional quiet period. Equal values at that floor cannot establish equal detection/repair speed. Read-only routing observations separately record convergence; lookup-triggered Chord repair can finish before SWIM confirmation.

Do not combine these results with older Docker/netem or standalone verifier reports: their fault models, workloads and timing differ.

## SWIM diagnostics

Summed local transitions per phase across nodes and repetitions. A single suspicion may be observed at several nodes; these are not unique physical failures. Baseline values are unavailable.

| Phase | SUSPECT transitions | Refutation observations | DEAD transitions | Validated reference writes |
|---|---:|---:|---:|---:|
| healthy | 0 | 0 | 0 | 0 |
| gray_failure | 51 | 67 | 0 | 0 |
| gray_recovered | 0 | 0 | 0 | 0 |
| crash_recovery | 9 | 0 | 9 | 4 |
| post_crash | 0 | 0 | 0 | 0 |
| restart | 0 | 0 | 0 | 0 |
