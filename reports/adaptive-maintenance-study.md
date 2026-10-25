# Adaptive Chord maintenance with standard SWIM

4 nodes, M=4, 3 paired repetitions per mode, 24 offered lookups/s. Each ordinary phase lasts 8 s; the crash observation lasts 6 s, with client lookups withheld for 2 s after the crash.

Fixed and adaptive SWIM runs use the same executable, initial Chord timers, probe settings and offered workloads. Only the adaptive flag differs. Plain Chord controls use the same executable in both modes. The report generator validates those conditions and per-phase query schedules before writing this report. Both variants receive the same phase duration; there is no adaptive-only settling period.

Initial stabilization/predecessor/finger intervals: 1000/1000/500 ms. Adaptive caps: 5000/5000/2000 ms; quiet threshold 2000 ms. Local quietness is a heuristic, not proof of global convergence.

Values in performance tables are medians of per-run values. Traffic rates divide each phase's destination-proxy counters by its configured duration; request and response body bytes exclude HTTP/TCP/IP headers. Latency p95 includes the harness's recorded requests. Whole-Go-process heap is not protocol-only memory.

| Mode | Variant | Healthy requests/s | Healthy body bytes/s | Healthy p95 ms | Mean process heap KiB |
|---|---|---:|---:|---:|---:|
| fixed | chord | 65.875 | 3888.6 | 8.331 | 1386.2 |
| fixed | chord_standard_swim | 85.125 | 11608.6 | 12.599 | 1848.3 |
| adaptive | chord | 66.750 | 3971.0 | 8.981 | 1875.1 |
| adaptive | chord_standard_swim | 70.625 | 10617.6 | 8.962 | 2038.3 |

| Mode | Variant | Gray oracle correct / queries | Gray correctness % | Gray requests/s | Gray body bytes/s | Gray p95 ms |
|---|---|---:|---:|---:|---:|---:|
| fixed | chord | 576/576 | 100.000 | 66.500 | 3945.1 | 109.571 |
| fixed | chord_standard_swim | 576/576 | 100.000 | 109.500 | 20823.6 | 110.667 |
| adaptive | chord | 576/576 | 100.000 | 66.000 | 3902.4 | 109.563 |
| adaptive | chord_standard_swim | 576/576 | 100.000 | 108.375 | 20863.1 | 109.773 |

## Recovery and correctness

Routing convergence requires two consecutive read-only observations with correct neighbors, successor lists and all fingers. A null time is censored at the observation window; it is not an infinite recovery time and is not silently excluded from an unconditional median.

| Mode | Variant | Trial | Full-routing convergence s | Converged before lookups s | All-phase oracle correct / queries | Acceptance | Processes exited |
|---|---|---:|---|---:|---:|---|---|
| fixed | chord | 1 | 2.109 | — | 1056/1056 | True | True |
| fixed | chord | 2 | 1.577 | 1.577 | 1056/1056 | True | True |
| fixed | chord | 3 | 3.560 | — | 1056/1056 | True | True |
| fixed | chord_standard_swim | 1 | 1.364 | 1.364 | 1056/1056 | True | True |
| fixed | chord_standard_swim | 2 | 1.360 | 1.360 | 1056/1056 | True | True |
| fixed | chord_standard_swim | 3 | 1.360 | 1.360 | 1056/1056 | True | True |
| adaptive | chord | 1 | 2.611 | — | 1056/1056 | True | True |
| adaptive | chord | 2 | 4.077 | — | 1056/1056 | True | True |
| adaptive | chord | 3 | 2.089 | — | 1056/1056 | True | True |
| adaptive | chord_standard_swim | 1 | 1.359 | 1.359 | 1056/1056 | True | True |
| adaptive | chord_standard_swim | 2 | 1.366 | 1.366 | 1056/1056 | True | True |
| adaptive | chord_standard_swim | 3 | 1.362 | 1.362 | 1056/1056 | True | True |

Oracle-correct lookups across every phase and control: **12672/12672**. These are sampled lookup outcomes, not proof of correctness for every key and execution history.

| Mode | Variant | Phase | Oracle correct / queries |
|---|---|---|---:|
| fixed | chord | healthy | 576/576 |
| fixed | chord | gray_failure | 576/576 |
| fixed | chord | gray_recovered | 576/576 |
| fixed | chord | crash_recovery | 288/288 |
| fixed | chord | post_crash | 576/576 |
| fixed | chord | restart | 576/576 |
| fixed | chord_standard_swim | healthy | 576/576 |
| fixed | chord_standard_swim | gray_failure | 576/576 |
| fixed | chord_standard_swim | gray_recovered | 576/576 |
| fixed | chord_standard_swim | crash_recovery | 288/288 |
| fixed | chord_standard_swim | post_crash | 576/576 |
| fixed | chord_standard_swim | restart | 576/576 |
| adaptive | chord | healthy | 576/576 |
| adaptive | chord | gray_failure | 576/576 |
| adaptive | chord | gray_recovered | 576/576 |
| adaptive | chord | crash_recovery | 288/288 |
| adaptive | chord | post_crash | 576/576 |
| adaptive | chord | restart | 576/576 |
| adaptive | chord_standard_swim | healthy | 576/576 |
| adaptive | chord_standard_swim | gray_failure | 576/576 |
| adaptive | chord_standard_swim | gray_recovered | 576/576 |
| adaptive | chord_standard_swim | crash_recovery | 288/288 |
| adaptive | chord_standard_swim | post_crash | 576/576 |
| adaptive | chord_standard_swim | restart | 576/576 |

## Observed adaptive state and maintenance operations

Boundary snapshots come from each SWIM process's actual /metrics response, not the requested caps. Intervals are listed as stabilize/fix_fingers/check_predecessor in ms; ×N is the number of processes with that state. Snapshots do not describe every intervening policy transition; raw event logs retain those transitions.

| Mode | SWIM trial | Healthy start | Healthy end |
|---|---:|---|---|
| fixed | 1 | disabled:fast (1000/500/1000) ×4 | disabled:fast (1000/500/1000) ×4 |
| fixed | 2 | disabled:fast (1000/500/1000) ×4 | disabled:fast (1000/500/1000) ×4 |
| fixed | 3 | disabled:fast (1000/500/1000) ×4 | disabled:fast (1000/500/1000) ×4 |
| adaptive | 1 | enabled:fast (1000/500/1000) ×1; enabled:backoff (5000/2000/5000) ×2; enabled:backoff (4000/2000/4000) ×1 | enabled:backoff (5000/2000/5000) ×4 |
| adaptive | 2 | enabled:fast (1000/500/1000) ×2; enabled:backoff (4000/2000/4000) ×1; enabled:backoff (2000/1000/2000) ×1 | enabled:backoff (5000/2000/5000) ×4 |
| adaptive | 3 | enabled:fast (1000/500/1000) ×1; enabled:backoff (2000/1000/2000) ×3 | enabled:backoff (5000/2000/5000) ×4 |

Operation counts below are median summed local scheduler invocations per phase, rather than repaired-reference counts. A task invocation can perform no work or several RPCs. Plain Chord lacks these scheduler counters, so no corresponding values are inferred. Missing counters are shown as unavailable (—).

| Mode | SWIM phase | Stabilize runs | Finger runs | Predecessor runs | Backoff transitions | Fast transitions |
|---|---|---:|---:|---:|---:|---:|
| fixed | healthy | 32.0 | 64.0 | 32.0 | — | — |
| fixed | gray_failure | 32.0 | 64.0 | 32.0 | — | — |
| fixed | gray_recovered | 32.0 | 64.0 | 32.0 | — | — |
| fixed | crash_recovery | 18.0 | 36.0 | 18.0 | — | — |
| fixed | post_crash | 24.0 | 48.0 | 24.0 | — | — |
| fixed | restart | 32.0 | 64.0 | 32.0 | — | — |
| adaptive | healthy | 10.0 | 21.0 | 11.0 | 2.0 | — |
| adaptive | gray_failure | 30.0 | 59.0 | 31.0 | 0.0 | 4.0 |
| adaptive | gray_recovered | 16.0 | 32.0 | 16.0 | 4.0 | 0.0 |
| adaptive | crash_recovery | 15.0 | 28.0 | 15.0 | 3.0 | 3.0 |
| adaptive | post_crash | 4.0 | 12.0 | 4.0 | 0.0 | 0.0 |
| adaptive | restart | 11.0 | 23.0 | 14.0 | 3.0 | 0.0 |

## Interpretation

Healthy request rate fell from 85.125 to 70.625/s (17.0%). Payload-body traffic fell from 11608.6 to 10617.6 bytes/s (8.5%). The actual policy snapshots and operation counters above establish that periodic work was reduced.

Adaptive SWIM still used 1.06× plain Chord's request rate and 2.67× its payload-body traffic. This is progress toward replacing routine work, not an equal-budget or overall-superiority result. Healthy p95 improvements are observations from three trials, not a statistically established claim.

Correctness and recovery observations are reported separately above. There was no adaptive-only warm-up; early healthy traffic includes the policy's transition toward its capped intervals. These measurements cannot establish safety under arbitrary partitions or sustained churn.

Verification for this implementation: final SWIM `go test -race ./...` passed (17.416 s), `go vet ./...` passed, baseline race tests passed from cache, and 31 Python harness tests passed (0.763 s). Additional adaptive runtime/RPC race tests passed three repetitions. Builds and tests finished before timing began. Source and reports are local on `chord_standard_swim`; this increment has not been pushed.

## Scope and reproducibility

The fixed study and adaptive study execute sequentially; each uses alternating variant order across repetitions. Local scheduling, random finger selection and process startup affect timing. No statistical significance or general superiority claim follows from these runs. The gray fault is delayed application traffic, not packet loss or an arbitrary partition. CPU time and total wire bytes were not measured. Ordinary Chord maintenance and lookup fallback remain active, so routing convergence cannot be attributed exclusively to SWIM events.

- Plain Chord executable SHA256: `a882ff44e73fb22d7c3ef37eb892e3fd747cf2fff4dcb9faeb3aa6aa6a728c47`.
- SWIM executable SHA256 (both modes): `9d0ed5cef8bf7f187d5e14dda5ef1c35a36521eb320a83138a0ac8139ce439e2`.
- Raw data: `reports/adaptive-maintenance-fixed.json` and `reports/adaptive-maintenance-adaptive.json`; companion CSV/Markdown reports are generated by the shared benchmark harness.
- Regenerate: `python3 -m orchestrator.report_adaptive_maintenance`.
