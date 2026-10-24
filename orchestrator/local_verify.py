"""Verify actual Go processes without Docker: joins, hard crashes and restarts.

Build the node first, then run from the repository root:
python3 -m orchestrator.local_verify --binary /tmp/gossipmesh-chord-fixed
"""

import argparse
import concurrent.futures
import contextlib
import json
import os
import random
import socket
import subprocess
import tempfile
from pathlib import Path

from orchestrator import runner


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True)
    parser.add_argument("--m", type=int, default=160)
    args = parser.parse_args()
    binary = str(Path(args.binary).resolve())
    processes = []
    reservations = []
    result = {}
    with (
        tempfile.TemporaryDirectory(prefix="chord-local-") as directory,
        contextlib.ExitStack() as handles,
    ):
        try:
            for _ in range(8):
                reservation = socket.socket()
                reservation.bind(("127.0.0.1", 0))
                reservations.append(reservation)
            ports = [reservation.getsockname()[1] for reservation in reservations]

            def start(index):
                env = dict(os.environ)
                env.pop("NODE_ID", None)
                env.update(
                    PORT=str(ports[index]),
                    ADVERTISE_ADDR=f"127.0.0.1:{ports[index]}",
                    BOOTSTRAP_ADDR="" if index == 0 else f"127.0.0.1:{ports[0]}",
                    M=str(args.m),
                    R="4",
                    STABILIZE_INTERVAL_MS="10",
                    FIX_FINGERS_INTERVAL_MS="5",
                    CHECK_PRED_INTERVAL_MS="15",
                    RPC_TIMEOUT_MS="500",
                )
                log = handles.enter_context(
                    open(Path(directory) / f"node-{index}.log", "a")
                )
                return subprocess.Popen([binary], env=env, stdout=log, stderr=log)

            for index, reservation in enumerate(reservations):
                reservation.close()
                processes.append(start(index))
            runner.wait_for_cluster(ports)
            runner.wait_for_ring_stabilization(ports, max_seconds=60)

            def verify(label, live_ports):
                live = runner.get_live_nodes(live_ports)
                rng = random.Random(42)
                keys = [rng.randrange(1 << args.m) for _ in range(128)]
                keys += [node["id"] for node in live]
                probes = [(node, key) for node in live for key in keys]

                def check(probe):
                    node, key = probe
                    reply = runner.get_json(
                        f"http://127.0.0.1:{node['port']}/lookup?id={key}"
                    )
                    expected = runner.oracle_successor(live, key, args.m)
                    owner = reply.get("successor") if reply else None
                    return bool(
                        reply
                        and reply.get("success")
                        and owner
                        and int(owner["id"]) == expected
                        and owner["addr"]
                        == next(peer["addr"] for peer in live if peer["id"] == expected)
                    )

                with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
                    correct = sum(pool.map(check, probes))
                result[label] = {"queries": len(probes), "correct": correct}
                if correct != len(probes):
                    raise AssertionError(f"{label}: {correct}/{len(probes)} correct")

            verify("healthy", ports)
            # Select adjacent nodes in identifier order, preserving the bootstrap.
            ordered = runner.get_live_nodes(ports)
            bootstrap_index = next(
                i for i, node in enumerate(ordered) if node["port"] == ports[0]
            )
            crashed_ports = [
                ordered[(bootstrap_index + offset) % len(ordered)]["port"]
                for offset in (1, 2)
            ]
            for port in crashed_ports:
                processes[ports.index(port)].kill()
                processes[ports.index(port)].wait()
            survivors = [port for port in ports if port not in crashed_ports]
            runner.wait_for_ring_stabilization(survivors, max_seconds=60)
            verify("two_adjacent_crashes", survivors)
            for port in crashed_ports:
                index = ports.index(port)
                processes[index] = start(index)
            runner.wait_for_cluster(ports)
            runner.wait_for_ring_stabilization(ports, max_seconds=60)
            verify("restarted", ports)
        finally:
            for reservation in reservations:
                reservation.close()
            for process in processes:
                if process.poll() is None:
                    process.terminate()
            for process in processes:
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            for path in Path(directory).glob("*.log"):
                contents = path.read_text()
                if "DATA RACE" in contents or "panic:" in contents:
                    raise AssertionError(contents)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
