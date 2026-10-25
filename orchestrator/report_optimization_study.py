"""Summarize current staged benchmark reports without inventing missing results."""
import json
from pathlib import Path
from statistics import median


def main():
    lines = ['# Standard-SWIM overhead study', '',
             'Four nodes, three paired repetitions per stage. Stabilization and predecessor checks: 1000 ms; finger maintenance: 500 ms; SWIM suspicion: 800 ms. Client lookups pause for the first 2 s after a crash. Each healthy/gray/post-crash phase lasts 4 s at 24 lookups/s. Gray failure delays peer traffic to one live node by 100 ms.', '',
             'Values below are medians across repetitions. Payload bytes count request and response bodies observed by proxies, excluding HTTP/TCP/IP headers. Routing convergence requires two consecutive correct full-routing snapshots; lookup recovery uses two successful 250 ms cohorts and has a 2.5 s measurement floor in this quiet experiment.', '',
             '| Stage | Variant | Healthy requests | Healthy payload bytes | Healthy p95 ms | Gray p95 ms | Routing convergence s | Quiet convergence trials |',
             '|---|---|---:|---:|---:|---:|---:|---:|']
    total = correct = 0
    phase_rows = []
    for stage in range(4):
        path = Path(f'reports/optimization-stage{stage}.json')
        if not path.exists():
            raise SystemExit(f'Missing {path}')
        data = json.loads(path.read_text())
        lines_stage = []
        for phase in ['gray_failure', 'crash_recovery']:
            peers = [r['phases'][phase] for r in data['runs'] if r['variant'] == 'chord_standard_swim']
            phase_rows.append(f'| {stage} | {phase} | {median(p["peer_traffic"]["requests"] for p in peers):.0f} | {median(p["peer_traffic"]["request_body_bytes"] + p["peer_traffic"]["response_body_bytes"] for p in peers):.0f} |')
        for variant in ['chord', 'chord_standard_swim']:
            runs = [r for r in data['runs'] if r['variant'] == variant]
            healthy = [r['phases']['healthy'] for r in runs]
            recovery = [r['phases']['crash_recovery'] for r in runs]
            convergence = [p['routing_convergence_sec'] for p in recovery if p['routing_convergence_sec'] is not None]
            conv = f'{median(convergence):.3f}' if len(convergence) == len(runs) else f'{len(convergence)}/{len(runs)} observed'
            quiet = sum(p['quiet_routing_convergence_sec'] is not None for p in recovery)
            lines.append(f'| {stage} | {variant} | {median(p["peer_traffic"]["requests"] for p in healthy):.0f} | {median(p["peer_traffic"]["request_body_bytes"] + p["peer_traffic"]["response_body_bytes"] for p in healthy):.0f} | {median(p["latency_p95_ms"] for p in healthy):.3f} | {median(r["phases"]["gray_failure"]["latency_p95_ms"] for r in runs):.3f} | {conv} | {quiet}/{len(runs)} |')
            for r in runs:
                for p in r['phases'].values():
                    total += p['queries']
                    correct += p['correct']
            lines_stage.append(f'{variant}: accepted={all(r["acceptance_passed"] for r in runs)}, processes exited={all(r["processes_exited"] for r in runs)}, binary SHA256={runs[0]["binary_sha256"]}')
        Path(f'/tmp/optimization-stage{stage}-checks.txt').write_text('\n'.join(lines_stage))
    lines += ['', '| Stage | SWIM phase | Requests | Payload bytes |', '|---|---|---:|---:|', *phase_rows]
    lines += ['', f'Oracle-correct lookups across all stages and controls: **{correct}/{total}**.', '',
              'Stages: 0 = original implementation; 1 = targeted scheduling and conditional successor-list refresh; 2 = compact version-2 identity-table encoding; 3 = fresh-ACK sharing for predecessor checks, plus dirty-successor cleanup when ALIVE supersedes queued DEAD.', '',
              'Three repetitions and four nodes are exploratory evidence, not a statistical or large-ring claim. Random finger maintenance and local scheduling affect convergence. Gray delay is not packet loss or an arbitrary partition. Correct sampled lookups do not prove correctness for every possible history. CPU time and total network wire bytes were not measured.', '',
              'Raw JSON and CSV reports: `reports/optimization-stage{0,1,2,3}.{json,csv}`. Each includes per-query oracle outcomes, phase metrics, routing observations, events, configuration and executable SHA256 hashes.']
    lines += ['', '## Interpretation and verification', '',
              'Compared with stage 0, stage 3 reduced healthy payload bodies from 63,968 to 45,707 bytes (28.5%) and requests from 338 to 331 (2.1%). Gray-phase requests fell from 464 to 434 (6.5%) and bytes from 128,592 to 83,494 (35.1%). These are medians of small repeated experiments, not confidence intervals.', '',
              'Median no-op repair jobs per SWIM run fell from 17 in stage 0 to 1 in stage 1. Compact encoding accounts for most of the byte savings; fresh-ACK sharing saves only a few periodic predecessor checks. No pre-crash DEAD transitions were observed in the 12 SWIM trials. Healthy p95 was 12.093 ms in stage 0 and 10.487 ms in stage 3; plain Chord stage 3 was 8.597 ms. No general latency or recovery improvement is established: stage-3 routing convergence (1.568 s) was close to its plain control (1.582 s), and slower than stage-0 SWIM (1.359 s).', '',
              'The optimized implementation remains more expensive than plain Chord: stage-3 healthy requests were 331 versus 259, and payload bytes 45,707 versus 15,239. This supports reduced SWIM overhead, not an overall efficiency win over Chord.', '',
              'Verification: baseline `go test -race ./...` passed (111.409 s); final SWIM `go test -race ./...` passed (16.655 s), `go vet ./...` passed, and the Python harness regression suite passed 25 tests. Stage binaries were built before the timing study. No builds or Go tests ran during the study.', '',
              'After the four-node timed study, a further correctness fix retained outstanding successor-list validation across retries after the dead reference was removed. A dedicated HTTP regression test verifies a failed validation is actually retried. The final source therefore differs from stage 3 by this retry fix; the eight-node final report, when present, measures the final binary. No four-node timing gain is attributed to the retry fix.', '',
              'Wire limitation: upgraded endpoints accept legacy v1 requests and return legacy responses, but new v2 clients require upgraded servers. Fresh ACK reuse is bounded to 200 ms and never applies to lookup or repair commit validation. Arbitrary partitions, packet-loss workloads and large-scale behavior remain unverified.']
    final_path = Path('reports/optimization-final-eight-nodes.json')
    if final_path.exists():
        final = json.loads(final_path.read_text())
        lines += ['', '## Final eight-node smoke test', '',
                  'One paired run of the final retry-fixed binary, M=8, 32 lookups/s; other timers and faults match the staged study. This is a smoke test, not a repeated performance comparison.', '',
                  '| Variant | Oracle correct / queries | Healthy requests | Healthy payload bytes | Healthy p95 ms | Full-routing convergence after crash |',
                  '|---|---:|---:|---:|---:|---|']
        for run in final['runs']:
            healthy = run['phases']['healthy']
            traffic = healthy['peer_traffic']
            convergence = run['phases']['crash_recovery']['routing_convergence_sec']
            observed = f'{convergence:.3f} s' if convergence is not None else 'not observed within 6 s'
            count = sum(p['correct'] for p in run['phases'].values())
            queries = sum(p['queries'] for p in run['phases'].values())
            lines.append(f'| {run["variant"]} | {count}/{queries} | {traffic["requests"]} | {traffic["request_body_bytes"] + traffic["response_body_bytes"]} | {healthy["latency_p95_ms"]:.3f} | {observed} |')
        lines += ['', 'Both runs passed acceptance and exited all processes. Plain Chord eventually converged during the subsequent convergence wait; a null six-second observation is not an infinite recovery time. The final SWIM run had no pre-crash DEAD transitions and all seven survivors confirmed the crashed peer. Five finger entries were recomputed by the coordinator during the crash window.', '',
                  'Observed final-build trace, local node 96, failed peer 128 (times relative to crash-phase start):', '',
                  '```text',
                  '+0.000 s  crash peer 128',
                  '+0.308 s  peer_suspected',
                  '+1.108 s  peer_dead',
                  '+1.114 s  finger 1 repaired: start=98, owner=160',
                  '+1.118 s  finger 2 repaired: start=100, owner=160',
                  '+1.125 s  finger 3 repaired: start=104, owner=160',
                  '+1.130 s  finger 5 repaired: start=128, owner=160; repair_completed',
                  '+1.435 s  full routing convergence observed across survivors',
                  '+2.000 s  client lookup traffic resumes',
                  '```', '',
                  'Ordinary Chord maintenance had already handled the immediate successor in this trace; the coordinator performed the listed finger repairs before client traffic resumed. Do not attribute every repaired reference or convergence improvement exclusively to SWIM.', '',
                  'Final source builds with `(cd chord_standard_swim && go build -o /tmp/chord-swim-final .)`. Repeat this smoke test with:', '',
                  '```sh',
                  'python3 -m orchestrator.compare_variants --chord-binary /tmp/chord-compare-baseline --swim-binary /tmp/chord-swim-final --nodes 8 --m 8 --rate 32 --stabilize-ms 1000 --check-pred-ms 1000 --fix-ms 500 --swim-suspicion-ms 800 --crash-quiet-ms 2000 --recovery-window-ms 250 --repeats 1 --output reports/optimization-final-eight-nodes.json',
                  '```', '',
                  'All staged and final smoke-test lookups: 15,360/15,360 oracle correct. Reports and source changes remain local on `chord_standard_swim`; these changes have not been pushed.']
    Path('reports/optimization-study.md').write_text('\n'.join(lines)+'\n')
    print('\n'.join(lines))


if __name__ == '__main__':
    main()
