# Stabilization interval: 200 ms versus 500 ms

Three paired trials per interval; four nodes, 16-key ring. Binaries and offered lookup schedules match across both experiments. Only stabilization changes: predecessor checking remains 200 ms, finger fixing 100 ms, and SWIM settings remain unchanged. The 200 ms trials were run earlier; this is a descriptive comparison across sequential experiments, not a randomized interval-level experiment.

## Healthy operation

Medians of three run summaries. Each ordinary phase offers 96 lookups over four seconds.

| Variant | Stabilize ms | Correct % | p95 ms | Peer requests | Peer body bytes |
|---|---:|---:|---:|---:|---:|
| chord | 200 | 100.0 | 17.125 | 622 | 38382 |
| chord_standard_swim | 200 | 100.0 | 21.685 | 700 | 87000 |
| chord | 500 | 100.0 | 11.346 | 528 | 26607 |
| chord_standard_swim | 500 | 100.0 | 11.900 | 608 | 75564 |

## Gray failure and hard-crash recovery

| Variant | Stabilize ms | Gray correct % | Gray p95 ms | Recovery correct % | Sustained recovery s | Post-crash p95 ms |
|---|---:|---:|---:|---:|---:|---:|
| chord | 200 | 100.000 | 111.403 | 100.000 | 2.000 | 13.515 |
| chord_standard_swim | 200 | 100.000 | 112.310 | 100.000 | 2.000 | 11.662 |
| chord | 500 | 100.000 | 109.689 | 100.000 | 2.000 | 11.049 |
| chord_standard_swim | 500 | 100.000 | 110.907 | 100.000 | 2.000 | 9.086 |

## Traffic changes

- chord, healthy requests: 622 → 528 (-15.1%).
- chord, healthy body_bytes: 38382 → 26607 (-30.7%).
- chord_standard_swim, healthy requests: 700 → 608 (-13.1%).
- chord_standard_swim, healthy body_bytes: 87000 → 75564 (-13.1%).
- Project comparison, SWIM at 500 ms versus Chord at 200 ms, healthy requests: 622 → 608 (-2.3%).
- Project comparison, SWIM at 500 ms versus Chord at 200 ms, healthy body_bytes: 38382 → 75564 (+96.9%).

## Interpretation and limits

- Scheduled stabilization frequency falls by 60%; total traffic also includes finger maintenance, predecessor checks, lookups and SWIM.
- The recovery statistic has a two-second floor. A tie at two seconds cannot distinguish subsecond recovery.
- These are small local trials with a 100 ms application-level gray delay and fast-error hard crashes, not packet-loss or silent-blackhole tests.
- Lookup correctness is oracle-checked. Latency differences across three trials do not establish statistical significance.
- A reduction in request count is not automatically a reduction in payload bytes. Both are reported; neither includes HTTP/TCP/IP overhead.
- The next study should sweep 200/500/1000/2000 ms for both variants, randomize interval order, and measure faster recovery cohorts and quiet-period proactive repairs. Finger fixing and predecessor checks remain additional maintenance costs.

## Reproduction

```bash
python3 -m orchestrator.compare_variants \
  --chord-binary /tmp/chord-compare-baseline \
  --swim-binary /tmp/chord-compare-swim \
  --stabilize-ms 500 --check-pred-ms 200 --fix-ms 100 \
  --repeats 3 --output reports/chord-swim-comparison-stabilize-500ms.json
```

Raw JSON/CSV and the full six-phase table are in `chord-swim-comparison-stabilize-500ms.*`. Original 200 ms outputs remain in `chord-swim-comparison.*`.
