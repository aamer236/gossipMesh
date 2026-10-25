"""Run: python3 -m visualizer.dashboard.server --port 8787"""
from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit

from visualizer.dashboard.runtime import Manager

ROOT = Path(__file__).resolve().parents[2]
ASSETS = Path(__file__).resolve().parent


def build_binaries(chord=None, swim=None, skip=False):
    directory = Path(tempfile.gettempdir()) / "gossipmesh-dashboard-bin"
    directory.mkdir(exist_ok=True)
    binaries = {"chord": str(Path(chord).resolve()) if chord else str(directory / "chord"), "swim": str(Path(swim).resolve()) if swim else str(directory / "swim")}
    for key, module in (("chord", "baseline_chord"), ("swim", "chord_standard_swim")):
        if not skip and not (chord if key == "chord" else swim):
            print(f"Building {module}…", flush=True)
            subprocess.run(["go", "build", "-o", binaries[key], "."], cwd=ROOT / module, check=True, timeout=120)
        if not Path(binaries[key]).is_file() or not os.access(binaries[key], os.X_OK):
            raise ValueError(f"Executable unavailable: {binaries[key]}")
    return binaries


def make_server(manager, port=8787):
    class Handler(BaseHTTPRequestHandler):
        server_version = "GossipMeshLab/1.0"

        def log_message(self, *args):
            pass

        def allowed_host(self):
            return self.headers.get("Host") in {f"127.0.0.1:{self.server.server_port}", f"localhost:{self.server.server_port}"}

        def send(self, status, body, content_type="application/json; charset=utf-8", export=False):
            payload = json.dumps(body, allow_nan=False).encode() if content_type.startswith("application/json") else body
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; base-uri 'none'")
            if export:
                self.send_header("Content-Disposition", 'attachment; filename="gossipmesh-live-run.json"')
            self.end_headers()
            try:
                self.wfile.write(payload)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def do_GET(self):
            if not self.allowed_host():
                self.send(403, {"error": "Use the local dashboard address"})
                return
            path = urlsplit(self.path).path
            if path == "/api/state":
                self.send(200, manager.snapshot())
            elif path == "/api/export":
                self.send(200, manager.snapshot(export=True), export=True)
            else:
                assets = {"/": ("index.html", "text/html; charset=utf-8"), "/app.js": ("app.js", "text/javascript; charset=utf-8"), "/style.css": ("style.css", "text/css; charset=utf-8")}
                asset = assets.get(path)
                if asset:
                    self.send(200, (ASSETS / asset[0]).read_bytes(), asset[1])
                else:
                    self.send(404, {"error": "Not found"})

        def do_POST(self):
            origin = self.headers.get("Origin")
            if not self.allowed_host() or (origin and origin != f"http://{self.headers.get('Host')}") or self.headers.get("Sec-Fetch-Site") == "cross-site":
                self.send(403, {"error": "Dashboard controls require a same-origin local request"})
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if not 0 < length <= 65536 or self.headers.get("Content-Type", "").split(";")[0] != "application/json":
                    raise ValueError("A bounded JSON request is required")
                self.connection.settimeout(3)
                data = json.loads(self.rfile.read(length))
                path = urlsplit(self.path).path
                if path == "/api/start":
                    result = manager.start(data)
                elif path == "/api/stop":
                    if data != {}:
                        raise ValueError("Stop takes no arguments")
                    result = manager.stop()
                elif path == "/api/fault":
                    result = manager.act(data)
                else:
                    self.send(404, {"error": "Not found"})
                    return
                self.send(200, result)
            except (ValueError, TypeError, KeyError, OSError, RuntimeError, subprocess.SubprocessError) as error:
                self.send(400, {"error": str(error)})

    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    server.daemon_threads = True
    return server


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8787)
    parser.add_argument("--chord-binary")
    parser.add_argument("--swim-binary")
    parser.add_argument("--skip-build", action="store_true")
    args = parser.parse_args()
    if not 0 <= args.port <= 65535:
        parser.error("port must be 0–65535")
    manager = Manager(build_binaries(args.chord_binary, args.swim_binary, args.skip_build))
    server = make_server(manager, args.port)
    interrupted = threading.Event()

    def shutdown(*_):
        if not interrupted.is_set():
            interrupted.set()
            threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)
    print(f"GossipMesh live lab: http://127.0.0.1:{server.server_port}", flush=True)
    try:
        server.serve_forever(poll_interval=.2)
    finally:
        try:
            manager.stop()
        finally:
            server.server_close()


if __name__ == "__main__":
    main()
