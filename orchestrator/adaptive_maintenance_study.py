"""Compare fixed and adaptive scheduling with identical paired local workloads."""
import argparse
import subprocess
import sys
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--chord-binary', default='/tmp/chord-compare-baseline')
    parser.add_argument('--swim-binary', default='/tmp/chord-swim-adaptive')
    parser.add_argument('--repeats', type=int, default=3)
    args = parser.parse_args()
    for mode in ('fixed', 'adaptive'):
        output = Path(f'reports/adaptive-maintenance-{mode}.json')
        if output.exists():
            raise SystemExit(f'{output} already exists; move it before starting a new study')
        command = [sys.executable, '-u', '-m', 'orchestrator.compare_variants',
                   '--chord-binary', args.chord_binary, '--swim-binary', args.swim_binary,
                   '--stabilize-ms', '1000', '--check-pred-ms', '1000', '--fix-ms', '500',
                   '--swim-suspicion-ms', '800', '--crash-quiet-ms', '2000',
                   '--recovery-window-ms', '250', '--phase-seconds', '8',
                   '--repeats', str(args.repeats), '--output', str(output)]
        if mode == 'adaptive':
            command += ['--swim-adaptive-maintenance']
        print(f'MAINTENANCE {mode}', flush=True)
        subprocess.run(command, check=True)


if __name__ == '__main__':
    main()
