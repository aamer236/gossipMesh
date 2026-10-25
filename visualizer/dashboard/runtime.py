"""Bounded two-ring local process controller; the oracle never supplies routes."""
from __future__ import annotations

import concurrent.futures
import contextlib
import copy
import datetime as dt
import hashlib
import json
import math
import random
import secrets
import socket
import subprocess
import tempfile
import threading
import time
import urllib.request
import urllib.error
from collections import deque
from pathlib import Path
from types import SimpleNamespace

from orchestrator.compare_variants import PeerProxy, process_environment, query, variant_config
from orchestrator.runner import get_json, ring_converged

KEYS = ("chord", "swim")
LABELS = {"chord": "Plain Chord", "swim": "Chord + SWIM"}
PROFILES = {"balanced": (1000, 500, 1000), "fast": (200, 100, 300), "slow": (5000, 2000, 5000)}
EVENT_TYPES = {"peer_suspected", "peer_refuted", "peer_dead", "peer_rejoined", "peer_added", "finger_dirty", "finger_repaired", "successor_repair_validated", "predecessor_cleared", "repair_completed", "repair_pending", "membership_event_handled", "references_identified", "successor_list_refreshed", "maintenance_fast", "maintenance_backoff", "error"}


def configuration(data):
    if not isinstance(data, dict) or set(data) - {"nodes", "rate", "profile", "adaptive"}:
        raise ValueError("Unknown experiment setting")
    result = {"nodes": data.get("nodes", 4), "rate": data.get("rate", 8), "profile": data.get("profile", "balanced"), "adaptive": data.get("adaptive", True)}
    for field, lower, upper in (("nodes", 2, 64), ("rate", 1, 32)):
        if type(result[field]) is not int or not lower <= result[field] <= upper:
            raise ValueError(f"{field} must be an integer from {lower} to {upper}")
    if not isinstance(result["profile"], str) or result["profile"] not in PROFILES or type(result["adaptive"]) is not bool:
        raise ValueError("Invalid maintenance profile or adaptive setting")
    return result


def percentile(values, fraction=.95):
    return sorted(values)[max(0, math.ceil(len(values) * fraction) - 1)] if values else None


def correct_ring(states, peers):
    """Also check exact expected identities: a smaller accidental ring is invalid."""
    if not peers or len(states) != len(peers) or not all(states):
        return False
    if {(int(s["id"]), s["addr"]) for s in states} != {(p["id"], p["addr"]) for p in peers}:
        return False
    return ring_converged(states)


def reference_count(state, live):
    if not state:
        return None
    refs = [state.get("successor"), state.get("predecessor"), *state.get("successor_list", []), *state.get("fingers", [])]
    return sum(bool(ref and (int(ref["id"]), ref["addr"]) not in live) for ref in refs)


class Experiment:
    def __init__(self, binaries, config):
        self.binaries = binaries
        self.config = config
        self.lock = threading.RLock()
        self.control = threading.Lock()
        self.cancel = threading.Event()
        self.directory = tempfile.TemporaryDirectory(prefix="gossipmesh-dashboard-")
        self.epoch = time.monotonic()
        self.wall_epoch = dt.datetime.now(dt.timezone.utc)
        self.status, self.error = "starting", None
        self.generation = 0
        self.node_ids = [i * 256 // config["nodes"] for i in range(config["nodes"])]
        self.rings = {}
        self.samples = {key: deque(maxlen=2400) for key in KEYS}
        self.totals = {key: {"queries": 0, "correct": 0, "excluded": 0, "overloads": 0} for key in KEYS}
        self.series = {key: deque(maxlen=600) for key in KEYS}
        self.events = deque(maxlen=240)
        self.event_seq = 0
        self.faults = []
        self.fault_seq = 0
        self.storage_operations = deque(maxlen=40)
        self.seed = secrets.randbits(32)
        self.seed_records = []
        self.seed_complete = False
        self.read_offsets = {}
        self.log_lock = threading.Lock()
        self.last_traffic = {}
        self.last_tick = None
        self.last_snapshot = {key: {"nodes": [], "summary": {}, "series": []} for key in KEYS}
        self.workers = concurrent.futures.ThreadPoolExecutor(max_workers=32, thread_name_prefix="dashboard-lookup")
        self.slots = {key: threading.BoundedSemaphore(16) for key in KEYS}
        self.thread = threading.Thread(target=self._run, name="dashboard-monitor", daemon=True)
        self.workload_thread = None
        self.hashes = {key: hashlib.sha256(Path(path).read_bytes()).hexdigest() for key, path in binaries.items()}
        self._event("experiment_start", detail=f"Starting two {config['nodes']}-node rings")
        self.thread.start()

    def elapsed(self):
        return time.monotonic() - self.epoch

    def _event(self, kind, variant=None, node=None, detail="", observed_time=None):
        with self.lock:
            self.event_seq += 1
            record = {"id": self.event_seq, "t": self.elapsed(), "type": kind, "variant": variant, "node": node, "detail": detail, "observed_time": observed_time}
            self.events.append(record)
            if variant == "swim" and node is not None:
                for fault in reversed(self.faults):
                    if fault["type"] == "crash" and str(fault["node"]) == str(node):
                        # Only the current crash episode collects evidence. A restart
                        # or any later action ends the previous episode.
                        if not fault.get("evidence_closed"):
                            fault.setdefault("evidence", []).append(copy.deepcopy(record))
                            fault["evidence"] = fault["evidence"][-256:]
                        break

    def _args(self):
        stabilize, fix, predecessor = PROFILES[self.config["profile"]]
        return SimpleNamespace(m=8, nodes=self.config["nodes"], stabilize_ms=stabilize, fix_ms=fix, check_pred_ms=predecessor, swim_suspicion_ms=5000 if self.config["nodes"] > 8 else 800, swim_interval_ms=1000 if self.config["nodes"] > 8 else 200, swim_direct_ms=250 if self.config["nodes"] > 8 else 50, rpc_timeout_ms=1000 if self.config["nodes"] > 8 else 250, swim_adaptive_maintenance=self.config["adaptive"], adaptive_stable_after_ms=2000, adaptive_max_stabilize_ms=5000, adaptive_max_fix_ms=2000, adaptive_max_check_pred_ms=5000)

    def _spawn(self, key, index):
        nodes = self.rings[key]
        node = nodes[index]
        other = next((n for n in nodes if n is not node and n.get("process") and n["process"].poll() is None), None)
        env = process_environment(variant_config("chord" if key == "chord" else "chord_standard_swim", self._args()))
        env.update(PORT=str(node["port"]), NODE_ID=str(node["id"]), ADVERTISE_ADDR=node["addr"], BOOTSTRAP_ADDR=other["addr"] if other else "", SWIM_STATE_FILE=str(Path(self.directory.name) / f"{key}-{index}.state"))
        log = node["log_path"].open("a")
        try:
            process = subprocess.Popen([self.binaries[key]], env=env, stdout=log, stderr=log)
        finally:
            log.close()
        node["process"] = process
        node["expected_alive"] = True
        node["ready"] = False

    def _allocate(self):
        for key in KEYS:
            self.rings[key] = []
            for index, ident in enumerate(self.node_ids):
                reservation = socket.socket()
                reservation.bind(("127.0.0.1", 0))
                port = reservation.getsockname()[1]
                proxy = PeerProxy(port)
                self.rings[key].append({"id": ident, "port": port, "addr": f"127.0.0.1:{proxy.port}", "proxy": proxy, "reservation": reservation, "process": None, "expected_alive": False, "ready": False, "delay_ms": 0, "log_path": Path(self.directory.name) / f"{key}-{index}.log"})
            # Bootstrap must be listening before starting the other participants.
            for index in range(len(self.node_ids)):
                if self.cancel.is_set():
                    return
                node = self.rings[key][index]
                node.pop("reservation").close()
                self._spawn(key, index)
                if index == 0:
                    until = time.monotonic() + 8
                    while not self.cancel.is_set():
                        if get_json(f"http://127.0.0.1:{node['port']}/health", timeout=.25):
                            break
                        if node["process"].poll() is not None or time.monotonic() > until:
                            raise RuntimeError(f"{LABELS[key]} bootstrap failed; inspect exported events")
                        self.cancel.wait(.05)

    def _live(self, key):
        return [n for n in self.rings.get(key, []) if n.get("expected_alive") and n["process"] and n["process"].poll() is None]

    def _run(self):
        try:
            self._allocate()
            startup_deadline = time.monotonic() + 300
            while not self.cancel.is_set():
                began = time.monotonic()
                self._logs()
                self._observe()
                with self.lock:
                    if self.status == "starting" and all(self.last_snapshot[k]["summary"].get("converged") for k in KEYS):
                        self.status = "seeding"
                    if self.status == "starting" and time.monotonic() > startup_deadline:
                        raise RuntimeError("Ring convergence not observed within 300 s; no lookup workload was started")
                if self.status == "seeding":
                    self._seed_storage()
                    if self.cancel.is_set():
                        break
                    self._observe()
                    with self.lock:
                        self.seed_complete = True
                        self.status = "running"
                        self.generation += 1
                        self._event("ready", detail="Both rings converged and shared random records stored; lookup workload started")
                        self.workload_thread = threading.Thread(target=self._workload, name="dashboard-workload", daemon=True)
                        self.workload_thread.start()
                self.cancel.wait(max(0, 1 - (time.monotonic() - began)))
        except Exception as error:
            with self.lock:
                self.error, self.status = str(error), "error"
                self._event("error", detail=str(error))
            self.cancel.set()
        finally:
            self.cancel.set()
            if self.workload_thread:
                self.workload_thread.join(timeout=3)
            self.workers.shutdown(wait=True, cancel_futures=True)
            self._cleanup()
            with self.lock:
                if self.status != "error":
                    self.status = "stopped"
                self._event("stopped", detail="Owned node processes and proxy listeners stopped")

    def _workload(self):
        tick, epoch = 0, time.monotonic()
        while not self.cancel.is_set():
            deadline = epoch + tick / self.config["rate"]
            if self.cancel.wait(max(0, deadline - time.monotonic())):
                return
            # A fault changes both oracle views while this small critical section
            # is excluded. A response spanning that change is explicitly excluded.
            with self.lock:
                generation = self.generation
                alive = {key: list(self._live(key)) for key in KEYS}
                common = [ident for ident in self.node_ids if all(any(n["id"] == ident and n["ready"] for n in alive[k]) for k in KEYS)]
                if not common:
                    tick += 1
                    continue
                ident = common[tick % len(common)]
                target = {key: next(n for n in alive[key] if n["id"] == ident) for key in KEYS}
                key_id = (tick * 37) % 256
                acquired = []
                for key in KEYS:
                    if self.slots[key].acquire(blocking=False):
                        acquired.append(key)
                    else:
                        break
                if len(acquired) != 2:
                    for key in acquired:
                        self.slots[key].release()
                    for key in KEYS:
                        self._record(key, {"t": self.elapsed(), "correct": False, "latency_ms": None, "key": key_id, "harness_overload": True}, generation)
                else:
                    for key in KEYS:
                        self.workers.submit(self._lookup, key, dict(target[key]), key_id, [dict(n) for n in alive[key]], generation)
            tick += 1
            # Do not issue a catch-up burst after host stalls; record skipped
            # paired offers instead of silently increasing the offered rate.
            due = int((time.monotonic() - epoch) * self.config["rate"])
            if due > tick + self.config["rate"]:
                with self.lock:
                    for key in KEYS:
                        self.totals[key]["overloads"] += due - tick
                tick = due

    def _lookup(self, variant, peer, key_id, alive, generation):
        try:
            sample = query(peer, key_id, alive, self.epoch, self.elapsed())
            sample["t"] = self.elapsed()
            self._record(variant, sample, generation)
        except Exception as error:
            self._record(variant, {"t": self.elapsed(), "correct": False, "latency_ms": None, "key": key_id, "error": str(error)}, generation)
        finally:
            self.slots[variant].release()

    def _record(self, variant, sample, generation):
        with self.lock:
            if generation != self.generation:
                self.totals[variant]["excluded"] += 1
                return
            self.samples[variant].append(sample)
            self.totals[variant]["queries"] += 1
            self.totals[variant]["correct"] += bool(sample["correct"])
            self.totals[variant]["overloads"] += bool(sample.get("harness_overload"))

    def _logs(self):
        with self.log_lock:
            self._read_logs()

    def _read_logs(self):
        for key, nodes in self.rings.items():
            for node in nodes:
                path = node["log_path"]
                if not path.exists():
                    continue
                with path.open() as handle:
                    handle.seek(self.read_offsets.get(str(path), 0))
                    for _ in range(1500):
                        line_start = handle.tell()
                        line = handle.readline()
                        if line and not line.endswith("\n"):
                            handle.seek(line_start)
                            break
                        if not line:
                            break
                        try:
                            event = json.loads(line)
                        except (ValueError, TypeError):
                            continue
                        if event.get("type") in EVENT_TYPES:
                            detail = event.get("details") or {}
                            self._event(event["type"], key, event.get("target_id"), {"local_id": event.get("local_id"), **detail}, event.get("time"))
                    self.read_offsets[str(path)] = handle.tell()

    def _observe(self):
        now = self.elapsed()
        for key in KEYS:
            with self.lock:
                generation = self.generation
                nodes = list(self.rings.get(key, []))
                live = list(self._live(key))
            observed = []
            for node in nodes:
                if node["expected_alive"] and node["process"] and node["process"].poll() is not None:
                    raise RuntimeError(f"{LABELS[key]} node {node['id']} exited unexpectedly")
                alive = node in live
                state = get_json(f"http://127.0.0.1:{node['port']}/state", timeout=.3) if alive else None
                metrics = get_json(f"http://127.0.0.1:{node['port']}/metrics", timeout=.3) if alive else None
                stored = get_json(f"http://127.0.0.1:{node['port']}/kv/local", timeout=.5) if alive else None
                membership = get_json(f"http://127.0.0.1:{node['port']}/membership", timeout=.3) if alive and key == "swim" else None
                if alive and node["process"].poll() is not None:
                    self._event("process_exit", key, node["id"], "Process exited unexpectedly")
                node["ready"] = bool(state)
                observed.append({"id": node["id"], "alive": alive, "reachable": bool(state), "delay_ms": node["delay_ms"], "addr": node["addr"], "state": state, "storage": stored, "maintenance": (metrics or {}).get("maintenance"), "membership": membership or [], "metrics": metrics})
            now = self.elapsed()
            live_identities = {(n["id"], n["addr"]) for n in live}
            states = [n["state"] for n in observed if n["alive"]]
            converged = correct_ring(states, live)
            traffic = {field: sum(n["proxy"].snapshot()[field] for n in nodes) for field in ("requests", "request_body_bytes", "response_body_bytes")}
            previous = self.last_traffic.get(key)
            interval = now - previous[0] if previous else None
            requests = (traffic["requests"] - previous[1]["requests"]) / interval if interval else None
            payload = (sum(traffic[f] - previous[1][f] for f in ("request_body_bytes", "response_body_bytes"))) / interval if interval else None
            self.last_traffic[key] = (now, traffic)
            with self.lock:
                if generation != self.generation:
                    continue
                samples = [s for s in self.samples[key] if now - 10 <= s["t"] <= now]
                latencies = [s["latency_ms"] for s in samples if s.get("latency_ms") is not None]
                references = [reference_count(n["state"], live_identities) for n in observed if n["alive"]]
                summary = {"alive": len(live), "converged": converged, "samples": len(samples), "correct_pct": 100 * sum(s["correct"] for s in samples) / len(samples) if samples else None, "p95_ms": percentile(latencies), "requests_s": requests, "payload_bytes_s": payload, "stale_references": sum(x for x in references if x is not None) if references and all(x is not None for x in references) else None, "totals": dict(self.totals[key]), "traffic_totals": traffic}
                point = {"t": now, **{field: summary[field] for field in ("correct_pct", "p95_ms", "requests_s", "payload_bytes_s", "stale_references")}}
                self.series[key].append(point)
                self.last_snapshot[key] = {"nodes": observed, "summary": summary, "series": list(self.series[key])}
                for fault in self.faults:
                    result = fault["recovery"][key]
                    if not fault.get("measure_recovery") or fault.get("interrupted") or result["seconds"] is not None or now < fault["t"]:
                        continue
                    result["consecutive"] = result["consecutive"] + 1 if converged else 0
                    if result["consecutive"] >= 2:
                        result["seconds"] = now - fault["t"]
                        self._event("routing_recovered", key, fault["node"], f"Full routing oracle correct in {result['seconds']:.2f} s")

    def act(self, action, ident, delay_ms=100):
        if type(ident) is not int or ident not in self.node_ids:
            raise ValueError("Choose a node in the current ring")
        if action not in ("crash", "restart", "delay", "heal"):
            raise ValueError("Unknown fault action")
        if type(delay_ms) is not int or not 0 <= delay_ms <= 500:
            raise ValueError("Delay must be from 0 to 500 ms")
        with self.control:
            self._logs()
            with self.lock:
                if self.status != "running":
                    raise ValueError("Wait for both rings to become ready")
                targets = {key: next(n for n in self.rings[key] if n["id"] == ident) for key in KEYS}
                if action == "crash":
                    if any(not n["expected_alive"] for n in targets.values()):
                        raise ValueError("Node is already crashed")
                    if any(len(self._live(key)) <= 1 for key in KEYS):
                        raise ValueError("Keep at least one surviving process per ring")
                if action == "restart" and any(n["process"].poll() is None for n in targets.values()):
                    raise ValueError("Crash the node before restarting it")
                if action in ("delay", "heal") and any(not n["expected_alive"] for n in targets.values()):
                    raise ValueError("Delay applies to a running process")
                self.generation += 1
                at = self.elapsed()
                for prior in self.faults:
                    prior["evidence_closed"] = True
                for fault in self.faults:
                    if fault.get("measure_recovery") and any(r["seconds"] is None for r in fault["recovery"].values()):
                        fault["interrupted"] = True
                self.fault_seq += 1
                fault = {"measure_recovery": action in ("crash", "restart"), "id": self.fault_seq, "type": action, "node": ident, "t": at, "recovery": {key: {"seconds": None, "consecutive": 0} for key in KEYS}, "interrupted": False, "evidence": [], "before": {key: [{"id": n["id"], "state": copy.deepcopy(n["state"])} for n in self.last_snapshot[key]["nodes"] if n["alive"] and n["state"]] for key in KEYS}}
                self.faults.append(fault)
                self.faults = self.faults[-40:]
                if action == "crash":
                    # Hold the oracle lock only around local kill operations,
                    # never around remote RPCs. Both targets change as one epoch.
                    for key, node in targets.items():
                        node["expected_alive"] = False
                        node["ready"] = False
                        node["process"].kill()
                        node["proxy"].set_delay(0)
                        node["delay_ms"] = 0
                elif action == "restart":
                    for key, node in targets.items():
                        node["proxy"].set_delay(0)
                        node["delay_ms"] = 0
                        self._spawn(key, self.rings[key].index(node))
                else:
                    for node in targets.values():
                        node["delay_ms"] = delay_ms if action == "delay" else 0
                        node["proxy"].set_delay(node["delay_ms"] / 1000)
                self._event(action, node=ident, detail=f"Applied to both rings{f' · {delay_ms} ms incoming peer delay' if action == 'delay' else ''}")
            if action == "crash":
                for node in targets.values():
                    node["process"].wait(timeout=2)
        return fault

    def _seed_storage(self):
        chosen = random.Random(self.seed).sample(range(256), min(128, max(16, 2*self.config["nodes"])))
        for index, key in enumerate(chosen):
            if self.cancel.is_set():
                return
            record = self.storage({"operation":"put", "key":key, "value":f"demo-{self.seed}-{key}", "initiator":self.node_ids[index % len(self.node_ids)]}, initializing=True)
            with self.lock:
                self.seed_records.append(copy.deepcopy(record))
            if any(r.get("http_status") != 200 or not r.get("found") for r in record["results"].values()):
                raise RuntimeError(f"Initial paired storage failed for key {key}; inspect exported seed records")
        self._event("storage_seed_complete", detail={"seed":self.seed,"records":len(chosen)})

    def storage(self, data, initializing=False):
        if not isinstance(data, dict) or set(data) - {"operation", "key", "value", "initiator"}:
            raise ValueError("Invalid storage operation")
        op, key = data.get("operation"), data.get("key")
        if op not in ("put", "get") or type(key) is not int or not 0 <= key < 256:
            raise ValueError("Choose put/get and an integer key from 0 to 255")
        value = data.get("value", "")
        if not isinstance(value, str) or len(value.encode()) > 65536:
            raise ValueError("Value must be a string of at most 64 KiB")
        with self.control:
            with self.lock:
                if self.status != "running" and not (initializing and self.status == "seeding"):
                    raise ValueError("Wait for convergence and initial storage to finish")
                ident = data.get("initiator", self.node_ids[0])
                if type(ident) is not int:
                    raise ValueError("Invalid initiating node")
                peers = {k: next((n for n in self._live(k) if n["id"] == ident), None) for k in KEYS}
                if not all(peers.values()):
                    raise ValueError("Choose an initiating node alive in both rings")
                record = {"t": self.elapsed(), "operation": op, "key": key, "initiator": ident, "sent_value": value if op == "put" else None, "results": {}}
            # Paired commands use the exact same key/value, never observer-supplied owners.
            for variant, peer in peers.items():
                payload = json.dumps({"value": value}).encode() if op == "put" else None
                request = urllib.request.Request(f"http://127.0.0.1:{peer['port']}/kv?id={key}", data=payload, method="PUT" if op == "put" else "GET", headers={"Content-Type": "application/json"})
                try:
                    with urllib.request.urlopen(request, timeout=5) as response:
                        result = {"http_status": response.status, **json.load(response)}
                except urllib.error.HTTPError as error:
                    with error:
                        result = {"http_status": error.code, **json.load(error)}
                except (OSError, ValueError) as error:
                    result = {"error": str(error)}
                record["results"][variant] = result
            with self.lock:
                self.storage_operations.append(record)
                self._event("storage_"+op, node=ident, detail={"key": key, "results": record["results"]})
        return record

    def snapshot(self, export=False):
        with self.lock:
            result = {"status": self.status, "storage_operations": copy.deepcopy(list(self.storage_operations)), "initial_storage": {"seed":self.seed,"complete":self.seed_complete,"records":copy.deepcopy(self.seed_records)}, "error": self.error, "config": self.config, "elapsed": self.elapsed(), "node_ids": self.node_ids, "variants": copy.deepcopy(self.last_snapshot), "events": list(self.events), "faults": copy.deepcopy(self.faults), "generation": self.generation, "provenance": {"binary_sha256": self.hashes, "protocol_settings": variant_config("chord_standard_swim", self._args()), "m": 8, "r": min(4, self.config["nodes"] - 1), "started_utc": self.wall_epoch.isoformat(), "oracle": "Mathematical successor over controller-known live IDs AND addresses", "correctness_window_sec": 10, "routing_recovery": "Two consecutive full-routing observations, nominally 1 s apart; client lookups continue", "traffic": "Proxy request attempts and request+response body bytes; excludes HTTP/TCP/IP headers and dashboard management traffic", "limitations": ["Both rings run concurrently on one host; interactive exploration, not an isolated benchmark", "Control polling and lookup traffic can affect timing and trigger Chord repairs", "Fault actions are paired; responses spanning a topology change are excluded and counted", "Ground truth process status differs from SWIM's local membership view", "Histories, events and samples are bounded; export cumulative counters and retained recent samples", "Full-routing recovery is distinct from lookup availability; null recovery is not observed, not infinite", "Delay injection is application-level incoming peer delay, not packet loss or arbitrary partitions"]}}
            if export:
                result["samples"] = {key: list(self.samples[key]) for key in KEYS}
            return result

    def stop(self):
        with self.lock:
            if self.status not in ("stopped", "error"):
                self.status = "stopping"
        self.cancel.set()
        self.thread.join(timeout=90)
        if self.thread.is_alive():
            raise RuntimeError("Experiment shutdown still in progress")

    def _cleanup(self):
        # Stop both rings before waiting, then release every owned listener.
        for nodes in self.rings.values():
            for node in nodes:
                if node.get("reservation"):
                    node["reservation"].close()
                process = node.get("process")
                if process and process.poll() is None:
                    process.terminate()
        for nodes in self.rings.values():
            for node in nodes:
                process = node.get("process")
                if process:
                    try:
                        process.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=2)
        # Closing servers sequentially costs up to 0.5 s per peer.
        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as closers:
            list(closers.map(lambda n: n["proxy"].close(), [n for nodes in self.rings.values() for n in nodes]))
        self._logs()
        self.directory.cleanup()


class Manager:
    def __init__(self, binaries):
        self.binaries = binaries
        self.experiment = None
        self.control = threading.Lock()

    def start(self, data):
        config = configuration(data)
        with self.control:
            if self.experiment and self.experiment.status not in ("stopped", "error"):
                raise ValueError("Stop the current experiment before starting another")
            if self.experiment:
                self.experiment.stop()
            self.experiment = Experiment(self.binaries, config)
        return self.snapshot()

    def stop(self):
        with self.control:
            if self.experiment:
                self.experiment.stop()
        return self.snapshot()

    def act(self, data):
        if not isinstance(data, dict) or set(data) - {"action", "node", "delay_ms"}:
            raise ValueError("Unknown fault setting")
        with self.control:
            if not self.experiment:
                raise ValueError("Start an experiment first")
            self.experiment.act(data.get("action"), data.get("node"), data.get("delay_ms", 100))
        return self.snapshot()

    def storage(self, data):
        with self.control:
            if not self.experiment:
                raise ValueError("Start an experiment first")
            self.experiment.storage(data)
        return self.snapshot()

    def snapshot(self, export=False):
        experiment = self.experiment
        return experiment.snapshot(export) if experiment else {"status": "idle", "elapsed": 0, "node_ids": [], "variants": {}, "events": [], "faults": [], "config": {"nodes": 4, "rate": 8, "adaptive": True, "profile": "balanced"}}
