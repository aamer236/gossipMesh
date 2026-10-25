# Slower Chord maintenance and SWIM suspicion study

Actual local runs: 24/24 accepted; 14400/14400 oracle-correct lookups. Every started node process exited.

Four conditions, each with three paired trials. Four nodes (IDs 0, 4, 8, 12), 16 keys, successor list three. Stabilization and predecessor checking: 1000ms. Finger fixing: 500ms. SWIM interval: 200ms; direct timeout: 50ms; two indirect helpers. Only suspicion duration and the intentional client quiet period vary between conditions.

The two node binaries are unchanged across all conditions. Paired variants have identical offered query schedules and Chord settings. Each pair alternates execution order across repetitions. Condition order is continuous/1200, continuous/800, quiet/1200, quiet/800, so this is a small descriptive study, not a randomized statistical experiment.

## Healthy cost and gray-failure behavior

Healthy numbers are medians across three runs. Correctness columns show the lowest phase correctness across those runs. Peer requests and request/response body bytes exclude management traffic and transport headers.

| Mode | Suspicion ms | Variant | Healthy correct % | Healthy p95 ms | Healthy requests | Healthy body bytes | Gray correct % | Gray p95 ms | False DEAD observations before crash |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|
| continuous | 1200 | chord | 100.000 | 7.677 | 262.000 | 15532.000 | 100.000 | 108.206 | — |
| continuous | 1200 | chord_standard_swim | 100.000 | 8.179 | 337.000 | 63869.000 | 100.000 | 109.099 | 0 |
| continuous | 800 | chord | 100.000 | 8.723 | 261.000 | 15429.000 | 100.000 | 108.010 | — |
| continuous | 800 | chord_standard_swim | 100.000 | 8.147 | 338.000 | 63968.000 | 100.000 | 108.931 | 0 |
| quiet | 1200 | chord | 100.000 | 7.807 | 257.000 | 15051.000 | 100.000 | 108.745 | — |
| quiet | 1200 | chord_standard_swim | 100.000 | 7.587 | 337.000 | 63873.000 | 100.000 | 108.242 | 0 |
| quiet | 800 | chord | 100.000 | 7.598 | 260.000 | 15338.000 | 100.000 | 107.967 | — |
| quiet | 800 | chord_standard_swim | 100.000 | 7.828 | 343.000 | 64441.000 | 100.000 | 108.632 | 0 |

## Lookup recovery versus complete routing-table recovery

Lookup recovery uses two consecutive 250ms cohorts with >=99% correctness per survivor; it has a 0.5s minimum under continuous queries. The quiet mode intentionally starts queries at 2s, so its lookup recovery has a 2.5s minimum. Full routing convergence is independently sampled by read-only /state calls roughly every 100ms and requires two consecutive correct snapshots of every finger, predecessor, successor and successor list.

| Mode | Suspicion ms | Variant | Crash correct % (worst run) | Lookup recovery s (median) | Full tables converged s (median of observed) | Observed within 6s | Converged during 2s quiet window | Validated coordinator writes before queries |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| continuous | 1200 | chord | 100.000 | 0.500 | 3.548 | 3/3 | — | — |
| continuous | 1200 | chord_standard_swim | 100.000 | 0.500 | 1.777 | 3/3 | — | — |
| continuous | 800 | chord | 100.000 | 0.500 | 1.988 | 3/3 | — | — |
| continuous | 800 | chord_standard_swim | 100.000 | 0.500 | 1.148 | 3/3 | — | — |
| quiet | 1200 | chord | 100.000 | 2.500 | 1.979 | 3/3 | 2/3 | — |
| quiet | 1200 | chord_standard_swim | 100.000 | 2.500 | 1.453 | 3/3 | 3/3 | 1 |
| quiet | 800 | chord | 100.000 | 2.500 | 3.006 | 3/3 | 0/3 | — |
| quiet | 800 | chord_standard_swim | 100.000 | 2.500 | 1.353 | 3/3 | 3/3 | 2 |

## Initial and restart convergence

Seconds, median (minimum–maximum). Initial timing starts after process launch. Restart timing includes waiting for membership/routing convergence after the replacement process is launched. These are separate startup phases, not lookup latencies.

| Mode | Suspicion ms | Variant | Initial convergence s | Restart convergence s |
|---|---:|---|---:|---:|
| continuous | 1200 | chord | 6.520 (6.040–7.522) | 4.857 (3.432–7.569) |
| continuous | 1200 | chord_standard_swim | 6.528 (6.034–8.093) | 5.742 (4.810–8.046) |
| continuous | 800 | chord | 6.085 (6.029–7.083) | 4.314 (3.897–5.057) |
| continuous | 800 | chord_standard_swim | 6.561 (6.019–7.034) | 4.858 (2.874–6.371) |
| quiet | 1200 | chord | 6.531 (6.031–8.024) | 5.416 (3.350–5.888) |
| quiet | 1200 | chord_standard_swim | 7.034 (5.044–8.524) | 2.875 (2.430–5.067) |
| quiet | 800 | chord | 6.513 (6.034–8.033) | 4.525 (2.553–9.372) |
| quiet | 800 | chord_standard_swim | 6.526 (5.554–9.035) | 3.429 (3.363–3.775) |

## Example background repair trace

Quiet trial 1, suspicion 800ms, local node 4, failed target 8. Client lookups start at +2.000s. Times are local-process wall-clock observations.

```text
+0.000s hard crash
+0.369s peer_suspected {"incarnation":6,"previous":"ALIVE","state":"SUSPECT"}
+1.210s peer_dead {"incarnation":6,"previous":"SUSPECT","state":"DEAD"}
+1.210s finger_dirty {"index":1}
+1.224s finger_repaired {"index":1,"owner":"12","start":"6"}
+1.224s repair_completed {"attempt_ms":13.249,"incarnation":6,"latency_ms":13.308,"result":{"repaired":1,"pending":false,"successor_promoted":false},"state":"DEAD"}
+2.000s client lookups begin
```

## What these results can establish

- Correct lookup ownership and a fully repaired finger table are different outcomes. Alternate Chord routes can keep lookups correct while fingers still reference a dead node.
- Quiet-mode state observations do not issue lookups. A validated coordinator write before +2s demonstrates background repair before client traffic resumes.
- A missing quiet convergence value does not mean the system failed permanently; it means full convergence was not observed before +2s. The six-second observation and subsequent post-crash convergence check are separate.
- Shorter suspicion may advance repair, but three local trials with zero false deaths cannot establish safety under arbitrary delays or partitions.
- The gray failure delays incoming RPCs by 100ms. Hard crashes produce fast local transport errors; no packet loss or silent blackhole is tested.
- The read-only observer adds management load to both variants during crash observation. Old 200/500ms experiments did not include this observer and used one-second recovery cohorts; compare their costs descriptively, not their recovery numbers directly.
- The study uses standard full-membership SWIM. It does not implement finger-scoped probing, remove Chord maintenance, change routing correctness rules, or optimize protocol payload encoding.
- All per-run samples, ranges, events, routing snapshots, metric deltas, binary hashes, and exact configurations are retained in the four JSON files and their CSV/Markdown companions.

## Reproduction and verification

Run the four commands in README.md under “Slower maintenance and shorter suspicion”. Each uses the same binaries and three repetitions. Python verification:

```bash
python3 -m unittest orchestrator.test_compare_variants orchestrator.test_standard_swim_verify orchestrator.test_runner -v
```

The experiment uses ordinary, matching Go builds. No Go code was changed by this study; race-instrumented node tests were not rerun for these performance measurements.
