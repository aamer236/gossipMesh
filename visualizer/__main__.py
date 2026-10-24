"""Local-only visualizer: python3 -m visualizer [--port 8765]."""

import argparse
import json
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any

from .simulation import Simulation

STATIC = Path(__file__).with_name("static")


class VisualizerServer(HTTPServer):
    """HTTP requests and scheduled maintenance share one serialized event loop."""

    def __init__(self, address: tuple[str, int]) -> None:
        self.simulation = Simulation()
        self.next_tick = time.monotonic() + self.simulation.interval
        super().__init__(address, Handler)

    def service_actions(self) -> None:
        now = time.monotonic()
        if now >= self.next_tick:
            if not self.simulation.paused:
                self.simulation.advance()
            # No catch-up burst after a slow request or computer suspension.
            self.next_tick = now + self.simulation.interval


class Handler(BaseHTTPRequestHandler):
    server: VisualizerServer

    def log_message(self, format: str, *args: Any) -> None:
        pass  # Quiet polling; unexpected errors still reach stderr.

    def _send(self, status: int, payload: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(payload)

    def _json(self, status: int, payload: Any) -> None:
        self._send(
            status, json.dumps(payload).encode(), "application/json; charset=utf-8"
        )

    def do_GET(self) -> None:
        if self.path == "/api/state":
            self._json(200, self.server.simulation.snapshot())
            return
        assets = {
            "/": ("index.html", "text/html; charset=utf-8"),
            "/app.js": ("app.js", "text/javascript; charset=utf-8"),
            "/style.css": ("style.css", "text/css; charset=utf-8"),
        }
        asset = assets.get(self.path)
        if asset is None:
            self._json(404, {"error": "Not found"})
            return
        self._send(200, (STATIC / asset[0]).read_bytes(), asset[1])

    def do_POST(self) -> None:
        # Mutations are same-origin JSON only. Do not accept form submissions
        # from arbitrary sites into this localhost demo.
        origin = self.headers.get("Origin")
        if origin and origin != f"http://{self.headers.get('Host')}":
            self._json(403, {"error": "Same-origin requests required"})
            return
        if self.headers.get("Content-Type") != "application/json":
            self._json(415, {"error": "JSON required"})
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if not 0 < length <= 1024:
                raise ValueError("Request body must be 1–1024 bytes")
            body = json.loads(self.rfile.read(length))
            if not isinstance(body, dict):
                raise ValueError("JSON object required")
            simulation = self.server.simulation
            identifier = body.get("id")
            if self.path in ("/api/crash", "/api/restore") and (
                not isinstance(identifier, int) or isinstance(identifier, bool)
            ):
                raise ValueError("node ID must be an integer")
            if self.path == "/api/crash":
                assert isinstance(identifier, int)
                simulation.crash(identifier)
            elif self.path == "/api/restore":
                assert isinstance(identifier, int)
                simulation.restore(identifier)
            elif self.path == "/api/step":
                simulation.advance()
            elif self.path == "/api/control":
                simulation.configure(
                    paused=body.get("paused"), interval=body.get("interval")
                )
            elif self.path == "/api/reset":
                simulation.reset()
            else:
                self._json(404, {"error": "Not found"})
                return
            self.server.next_tick = time.monotonic() + simulation.interval
            self._json(200, simulation.snapshot())
        except (ValueError, TypeError) as error:
            self._json(400, {"error": str(error)})


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    with VisualizerServer(("127.0.0.1", args.port)) as server:
        print(f"Chord visualizer: http://127.0.0.1:{server.server_port}", flush=True)
        try:
            server.serve_forever(poll_interval=0.1)
        except KeyboardInterrupt:
            print("\nVisualizer stopped.")


if __name__ == "__main__":
    main()
