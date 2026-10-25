"""Sequential paired measurements of immutable staged binaries (no builds during runs)."""
import argparse
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--binary-dir', default='/tmp')
    args = parser.parse_args()
    for stage in range(4):
        binary = f'{args.binary_dir}/chord-compare-swim' if stage == 0 else f'{args.binary_dir}/chord-swim-stage{stage}'
        command = [sys.executable, '-u', '-m', 'orchestrator.compare_variants',
                   '--chord-binary', f'{args.binary_dir}/chord-compare-baseline',
                   '--swim-binary', binary, '--stabilize-ms', '1000',
                   '--check-pred-ms', '1000', '--fix-ms', '500',
                   '--swim-suspicion-ms', '800', '--crash-quiet-ms', '2000',
                   '--recovery-window-ms', '250', '--repeats', str(args.repeats),
                   '--output', f'reports/optimization-stage{stage}.json']
        print(f'STAGE {stage}', flush=True)
        subprocess.run(command, check=True)


if __name__ == '__main__':
    main()
