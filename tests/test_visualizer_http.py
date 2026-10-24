import io
import json
import unittest
from types import SimpleNamespace

from visualizer.__main__ import Handler
from visualizer.simulation import Simulation


class MemoryConnection:
    """Exercise real HTTP parsing/handlers without sockets or background threads."""

    def __init__(self, data):
        self.input = io.BytesIO(data)
        self.output = bytearray()

    def makefile(self, *args):
        return self.input

    def sendall(self, data):
        self.output.extend(data)


class VisualizerHTTPTests(unittest.TestCase):
    def setUp(self):
        self.host = SimpleNamespace(simulation=Simulation(), next_tick=0)

    def request(
        self, method, path, body=None, *, origin=None, content_type="application/json"
    ):
        payload = json.dumps(body).encode() if body is not None else b""
        headers = [f"{method} {path} HTTP/1.0", "Host: 127.0.0.1:8765"]
        if payload:
            headers.extend(
                [f"Content-Length: {len(payload)}", f"Content-Type: {content_type}"]
            )
        if origin:
            headers.append(f"Origin: {origin}")
        connection = MemoryConnection(
            ("\r\n".join(headers) + "\r\n\r\n").encode() + payload
        )
        Handler(connection, ("127.0.0.1", 12345), self.host)
        head, data = bytes(connection.output).split(b"\r\n\r\n", 1)
        status = int(head.split()[1])
        return status, head, data

    def test_state_static_assets_and_unknown_routes(self):
        status, _, body = self.request("GET", "/api/state")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["online"], 32)
        for path, mime in (
            ("/", b"text/html"),
            ("/app.js", b"text/javascript"),
            ("/style.css", b"text/css"),
        ):
            status, headers, data = self.request("GET", path)
            self.assertEqual(status, 200)
            self.assertIn(mime, headers)
            self.assertGreater(len(data), 100)
        self.assertEqual(self.request("GET", "/../context.md")[0], 404)
        self.assertEqual(self.request("POST", "/missing", {})[0], 404)

    def test_crash_step_recovery_restore_reset_http_flow(self):
        self.assertEqual(self.request("POST", "/api/control", {"paused": True})[0], 200)
        status, _, body = self.request("POST", "/api/crash", {"id": 8})
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["online"], 31)
        self.assertGreater(json.loads(body)["stale_fingers"], 0)
        for _ in range(12):
            _, _, body = self.request("POST", "/api/step", {})
            if json.loads(body)["converged"]:
                break
        self.assertTrue(json.loads(body)["converged"])
        self.assertEqual(json.loads(body)["stale_fingers"], 0)
        _, _, body = self.request("POST", "/api/restore", {"id": 8})
        self.assertEqual(json.loads(body)["online"], 32)
        _, _, body = self.request("POST", "/api/reset", {})
        self.assertTrue(json.loads(body)["converged"])
        self.assertEqual(json.loads(body)["tick"], 0)

    def test_validation_does_not_mutate_simulation(self):
        for path, body in (
            ("/api/crash", {"id": True}),
            ("/api/crash", {"id": "8"}),
            ("/api/crash", {"id": 3}),
            ("/api/control", {"interval": -1}),
            ("/api/control", {"paused": "yes"}),
            ("/api/reset", []),
        ):
            self.assertEqual(self.request("POST", path, body)[0], 400)
        self.assertEqual(
            self.request("POST", "/api/crash", {"id": 8}, origin="https://example.com")[
                0
            ],
            403,
        )
        self.assertEqual(
            self.request("POST", "/api/reset", {}, content_type="text/plain")[0], 415
        )
        self.assertEqual(self.host.simulation.snapshot()["online"], 32)
