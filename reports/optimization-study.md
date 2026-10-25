# Standard-SWIM overhead study

Four nodes, three paired repetitions per stage. Stabilization and predecessor checks: 1000 ms; finger maintenance: 500 ms; SWIM suspicion: 800 ms. Client lookups pause for the first 2 s after a crash. Each healthy/gray/post-crash phase lasts 4 s at 24 lookups/s. Gray failure delays peer traffic to one live node by 100 ms.

Values below are medians across repetitions. Payload bytes count request and response bodies observed by proxies, excluding HTTP/TCP/IP headers. Routing convergence requires two consecutive correct full-routing snapshots; lookup recovery uses two successful 250 ms cohorts and has a 2.5 s measurement floor in this quiet experiment.

| Stage | Variant | Healthy requests | Healthy payload bytes | Healthy p95 ms | Gray p95 ms | Routing convergence s | Quiet convergence trials |
|---|---|---:|---:|---:|---:|---:|---:|
| 0 | chord | 266 | 15914 | 8.580 | 109.231 | 2.507 | 1/3 |
| 0 | chord_standard_swim | 338 | 63968 | 12.093 | 110.291 | 1.359 | 3/3 |
| 1 | chord | 258 | 15150 | 8.657 | 109.320 | 1.980 | 2/3 |
| 1 | chord_standard_swim | 338 | 63958 | 12.154 | 110.928 | 1.890 | 2/3 |
| 2 | chord | 258 | 15146 | 9.522 | 109.399 | 3.038 | 0/3 |
| 2 | chord_standard_swim | 336 | 45722 | 11.883 | 110.034 | 1.268 | 3/3 |
| 3 | chord | 259 | 15239 | 8.597 | 109.326 | 1.582 | 3/3 |
| 3 | chord_standard_swim | 331 | 45707 | 10.487 | 110.541 | 1.568 | 3/3 |

| Stage | SWIM phase | Requests | Payload bytes |
|---|---|---:|---:|
| 0 | gray_failure | 464 | 128592 |
| 0 | crash_recovery | 387 | 88427 |
| 1 | gray_failure | 438 | 127522 |
| 1 | crash_recovery | 383 | 88035 |
| 2 | gray_failure | 437 | 84389 |
| 2 | crash_recovery | 371 | 62439 |
| 3 | gray_failure | 434 | 83494 |
| 3 | crash_recovery | 369 | 63848 |

Oracle-correct lookups across all stages and controls: **13824/13824**.

Stages: 0 = original implementation; 1 = targeted scheduling and conditional successor-list refresh; 2 = compact version-2 identity-table encoding; 3 = fresh-ACK sharing for predecessor checks, plus dirty-successor cleanup when ALIVE supersedes queued DEAD.

Three repetitions and four nodes are exploratory evidence, not a statistical or large-ring claim. Random finger maintenance and local scheduling affect convergence. Gray delay is not packet loss or an arbitrary partition. Correct sampled lookups do not prove correctness for every possible history. CPU time and total network wire bytes were not measured.

Raw JSON and CSV reports: `reports/optimization-stage{0,1,2,3}.{json,csv}`. Each includes per-query oracle outcomes, phase metrics, routing observations, events, configuration and executable SHA256 hashes.

## Interpretation and verification

Compared with stage 0, stage 3 reduced healthy payload bodies from 63,968 to 45,707 bytes (28.5%) and requests from 338 to 331 (2.1%). Gray-phase requests fell from 464 to 434 (6.5%) and bytes from 128,592 to 83,494 (35.1%). These are medians of small repeated experiments, not confidence intervals.

Median no-op repair jobs per SWIM run fell from 17 in stage 0 to 1 in stage 1. Compact encoding accounts for most of the byte savings; fresh-ACK sharing saves only a few periodic predecessor checks. No pre-crash DEAD transitions were observed in the 12 SWIM trials. Healthy p95 was 12.093 ms in stage 0 and 10.487 ms in stage 3; plain Chord stage 3 was 8.597 ms. No general latency or recovery improvement is established: stage-3 routing convergence (1.568 s) was close to its plain control (1.582 s), and slower than stage-0 SWIM (1.359 s).

The optimized implementation remains more expensive than plain Chord: stage-3 healthy requests were 331 versus 259, and payload bytes 45,707 versus 15,239. This supports reduced SWIM overhead, not an overall efficiency win over Chord.

Verification: baseline `go test -race ./...` passed (111.409 s); final SWIM `go test -race ./...` passed (16.655 s), `go vet ./...` passed, and the Python harness regression suite passed 25 tests. Stage binaries were built before the timing study. No builds or Go tests ran during the study.

After the four-node timed study, a further correctness fix retained outstanding successor-list validation across retries after the dead reference was removed. A dedicated HTTP regression test verifies a failed validation is actually retried. The final source therefore differs from stage 3 by this retry fix; the eight-node final report, when present, measures the final binary. No four-node timing gain is attributed to the retry fix.

Wire limitation: upgraded endpoints accept legacy v1 requests and return legacy responses, but new v2 clients require upgraded servers. Fresh ACK reuse is bounded to 200 ms and never applies to lookup or repair commit validation. Arbitrary partitions, packet-loss workloads and large-scale behavior remain unverified.

## Final eight-node smoke test

One paired run of the final retry-fixed binary, M=8, 32 lookups/s; other timers and faults match the staged study. This is a smoke test, not a repeated performance comparison.

| Variant | Oracle correct / queries | Healthy requests | Healthy payload bytes | Healthy p95 ms | Full-routing convergence after crash |
|---|---:|---:|---:|---:|---|
| chord | 768/768 | 472 | 33064 | 16.120 | not observed within 6 s |
| chord_standard_swim | 768/768 | 646 | 99918 | 14.630 | 1.435 s |

Both runs passed acceptance and exited all processes. Plain Chord eventually converged during the subsequent convergence wait; a null six-second observation is not an infinite recovery time. The final SWIM run had no pre-crash DEAD transitions and all seven survivors confirmed the crashed peer. Five finger entries were recomputed by the coordinator during the crash window.

Observed final-build trace, local node 96, failed peer 128 (times relative to crash-phase start):

```text
+0.000 s  crash peer 128
+0.308 s  peer_suspected
+1.108 s  peer_dead
+1.114 s  finger 1 repaired: start=98, owner=160
+1.118 s  finger 2 repaired: start=100, owner=160
+1.125 s  finger 3 repaired: start=104, owner=160
+1.130 s  finger 5 repaired: start=128, owner=160; repair_completed
+1.435 s  full routing convergence observed across survivors
+2.000 s  client lookup traffic resumes
```

Ordinary Chord maintenance had already handled the immediate successor in this trace; the coordinator performed the listed finger repairs before client traffic resumed. Do not attribute every repaired reference or convergence improvement exclusively to SWIM.

Final source builds with `(cd chord_standard_swim && go build -o /tmp/chord-swim-final .)`. Repeat this smoke test with:

```sh
python3 -m orchestrator.compare_variants --chord-binary /tmp/chord-compare-baseline --swim-binary /tmp/chord-swim-final --nodes 8 --m 8 --rate 32 --stabilize-ms 1000 --check-pred-ms 1000 --fix-ms 500 --swim-suspicion-ms 800 --crash-quiet-ms 2000 --recovery-window-ms 250 --repeats 1 --output reports/optimization-final-eight-nodes.json
```

All staged and final smoke-test lookups: 15,360/15,360 oracle correct. Reports and source changes remain local on `chord_standard_swim`; these changes have not been pushed.
