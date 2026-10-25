# Chord / Chord + standard SWIM comparison

Generated 2026-10-24T23:42:07.209468+00:00. Acceptance passed: True.

Same offered lookup schedule, IDs, Chord timers, gray fault and hard-crash victim. Runs execute sequentially with alternating order.

Values below are medians of per-run results. Raw samples, ranges, counters and SWIM events are in the companion JSON.

| Phase | Variant | Runs | Correct % | p50 ms | p95 ms | p99 ms | Mean hops | Recovery s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| healthy | chord | 3 | 100.000 | 5.333 | 8.580 | 20.604 | 1.750 | — |
| gray_failure | chord | 3 | 100.000 | 7.003 | 109.231 | 124.771 | 1.750 | — |
| gray_recovered | chord | 3 | 100.000 | 5.312 | 8.794 | 17.922 | 1.750 | — |
| crash_recovery | chord | 3 | 100.000 | 5.215 | 8.025 | 9.362 | 1.604 | 2.500 |
| post_crash | chord | 3 | 100.000 | 5.180 | 7.451 | 19.120 | 1.604 | — |
| restart | chord | 3 | 100.000 | 5.331 | 8.555 | 9.705 | 1.750 | — |
| healthy | chord_standard_swim | 3 | 100.000 | 5.382 | 12.093 | 34.461 | 1.750 | — |
| gray_failure | chord_standard_swim | 3 | 100.000 | 7.952 | 110.291 | 114.287 | 1.750 | — |
| gray_recovered | chord_standard_swim | 3 | 100.000 | 5.276 | 9.421 | 15.197 | 1.750 | — |
| crash_recovery | chord_standard_swim | 3 | 100.000 | 5.265 | 8.092 | 10.113 | 1.604 | 2.500 |
| post_crash | chord_standard_swim | 3 | 100.000 | 5.435 | 8.232 | 22.322 | 1.604 | — |
| restart | chord_standard_swim | 3 | 100.000 | 5.610 | 9.168 | 11.786 | 1.750 | — |

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
| chord | 1 | 2.000 | 3.550 | — | 6.037 | 2.854 |
| chord_standard_swim | 1 | 2.000 | 1.359 | 1.359 | 6.091 | 4.054 |
| chord_standard_swim | 2 | 2.000 | 1.379 | 1.379 | 6.057 | 4.228 |
| chord | 2 | 2.000 | 2.507 | — | 7.524 | 4.545 |
| chord | 3 | 2.000 | 1.046 | 1.046 | 6.031 | 4.025 |
| chord_standard_swim | 3 | 2.000 | 1.259 | 1.259 | 6.539 | 4.867 |

## Peer traffic and memory

Per-phase medians across runs. Traffic is measured at proxies; Go heap is a whole-process snapshot averaged over active nodes. Neither includes proxy Python memory.

| Phase | Variant | Peer requests | Body bytes | Mean Go heap KiB |
|---|---|---:|---:|---:|
| healthy | chord | 266 | 15914 | 1643.1 |
| gray_failure | chord | 258 | 15148 | 2006.9 |
| gray_recovered | chord | 260 | 15338 | 1993.1 |
| crash_recovery | chord | 255 | 13212 | 1779.2 |
| post_crash | chord | 220 | 11502 | 1973.5 |
| restart | chord | 260 | 15338 | 1566.9 |
| healthy | chord_standard_swim | 338 | 63968 | 2394.0 |
| gray_failure | chord_standard_swim | 464 | 128592 | 2273.6 |
| gray_recovered | chord_standard_swim | 339 | 67602 | 2155.0 |
| crash_recovery | chord_standard_swim | 387 | 88427 | 2276.0 |
| post_crash | chord_standard_swim | 278 | 48093 | 2367.8 |
| restart | chord_standard_swim | 337 | 63993 | 2253.6 |

The recovery floor is twice the configured cohort duration plus any intentional quiet period. Equal values at that floor cannot establish equal detection/repair speed. Read-only routing observations separately record convergence; lookup-triggered Chord repair can finish before SWIM confirmation.

Do not combine these results with older Docker/netem or standalone verifier reports: their fault models, workloads and timing differ.

## SWIM diagnostics

Summed local transitions per phase across nodes and repetitions. A single suspicion may be observed at several nodes; these are not unique physical failures. Baseline values are unavailable.

| Phase | SUSPECT transitions | Refutation observations | DEAD transitions | Validated reference writes |
|---|---:|---:|---:|---:|
| healthy | 0 | 0 | 0 | 0 |
| gray_failure | 52 | 65 | 0 | 0 |
| gray_recovered | 0 | 0 | 0 | 0 |
| crash_recovery | 9 | 0 | 9 | 3 |
| post_crash | 0 | 0 | 0 | 0 |
| restart | 0 | 0 | 0 | 0 |
