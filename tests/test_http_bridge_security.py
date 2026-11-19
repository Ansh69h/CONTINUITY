import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from continuity import ContinuityRuntime, HttpBridge, InMemoryCausalStore, SideEffectClass


def test_http_bridge_rejects_hosts_outside_configured_allowlist_without_network_access():
    runtime = ContinuityRuntime(store=InMemoryCausalStore(), run_id="allowlist-test")
    bridge = HttpBridge(runtime, allowed_hosts=["payments.example"])

    with pytest.raises(ValueError, match="not in the allowed_hosts"):
        bridge.request(
            "charge", "POST", "http://127.0.0.1:9/charge", payload={"amount": 1},
            side_effect=SideEffectClass.IRREVERSIBLE,
        )

    assert runtime.store.get_events("allowlist-test") == []


@pytest.mark.parametrize(
    "request_options",
    [
        {"url": "http://blocked.example/charge"},
        {"url": "https://payments.example/charge", "reconcile_url": "http://blocked.example/status"},
        {"url": "https://payments.example/charge", "compensate_url": "http://blocked.example/reverse"},
    ],
)
def test_all_http_bridge_urls_are_validated_before_recording_step_history(request_options):
    runtime = ContinuityRuntime(store=InMemoryCausalStore(), run_id="preflight-validation")
    bridge = HttpBridge(runtime, allowed_hosts=["payments.example"])

    with pytest.raises(ValueError, match="not in the allowed_hosts"):
        bridge.request("charge", "POST", side_effect=SideEffectClass.COMPENSATABLE, **request_options)

    assert runtime.store.get_events("preflight-validation") == []


def test_http_bridge_caps_response_body_size():
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            body = b"12345"
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        bridge = HttpBridge(
            ContinuityRuntime(store=InMemoryCausalStore(), run_id="response-cap"),
            max_response_bytes=4,
        )
        with pytest.raises(ValueError, match="exceeds max_response_bytes"):
            bridge.request(
                "read",
                "GET",
                f"http://127.0.0.1:{server.server_port}/body",
                side_effect=SideEffectClass.PURE,
            )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_http_bridge_allowlist_is_enforced_after_redirects():
    class RedirectHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(302)
            self.send_header("Location", "http://example.invalid/redirect-target")
            self.end_headers()

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), RedirectHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        bridge = HttpBridge(
            ContinuityRuntime(store=InMemoryCausalStore(), run_id="redirect-allowlist"),
            allowed_hosts=["127.0.0.1"],
        )
        with pytest.raises(ValueError, match="not in the allowed_hosts"):
            bridge.request(
                "read",
                "GET",
                f"http://127.0.0.1:{server.server_port}/redirect",
                side_effect=SideEffectClass.PURE,
            )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_http_bridge_does_not_drain_large_redirect_bodies_before_following():
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/redirect":
                self.send_response(302)
                self.send_header("Location", "/final")
                self.send_header("Content-Length", "8192")
                self.end_headers()
                try:
                    self.wfile.write(b"x" * 16)
                    self.wfile.flush()
                    time.sleep(0.2)
                    self.wfile.write(b"x" * (8192 - 16))
                except (BrokenPipeError, ConnectionResetError):
                    pass
                return

            body = b'{"ok": true}'
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        bridge = HttpBridge(
            ContinuityRuntime(store=InMemoryCausalStore(), run_id="redirect-body-cap"),
            timeout=0.05,
            max_response_bytes=32,
        )
        result = bridge.request(
            "read",
            "GET",
            f"http://127.0.0.1:{server.server_port}/redirect",
            side_effect=SideEffectClass.PURE,
        )
        assert result["body"] == {"ok": True}
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
