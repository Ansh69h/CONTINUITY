from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs
import json
import socket
import threading
import time
from continuity import (
    InMemoryCausalStore,
    ContinuityRuntime,
    HttpBridge,
    SideEffectClass,
    InvariantChecker,
)


class AdversarialFaultHandler(BaseHTTPRequestHandler):
    """Real HTTP server injecting timeouts, lost acknowledgements, duplicate requests, and partial 503s."""

    state_lock = threading.Lock()
    attempts_by_key = {}
    committed_orders = {}

    def log_message(self, format, *args):
        return

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/reconcile":
            key = parse_qs(parsed.query).get("idempotency_key", [""])[0]
            with self.state_lock:
                record = self.committed_orders.get(key)
            if record is not None:
                body = json.dumps(record).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:
                self.send_response(404)
                self.end_headers()

    def do_POST(self):
        content_len = int(self.headers.get("Content-Length", "0"))
        raw_body = self.rfile.read(content_len).decode("utf-8") if content_len else "{}"
        payload = json.loads(raw_body)
        idem_key = self.headers.get("Idempotency-Key", "")

        if self.path == "/orders/lost-ack":
            with self.state_lock:
                count = self.attempts_by_key.get(idem_key, 0) + 1
                self.attempts_by_key[idem_key] = count
                if count == 1:
                    self.committed_orders[idem_key] = {"order_id": "ORD-991", "item": payload["item"], "commits": 1}
            if count == 1:
                time.sleep(0.35)
                return
            with self.state_lock:
                record = self.committed_orders[idem_key]
            body = json.dumps(record).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)

        elif self.path == "/orders/partial-failure":
            with self.state_lock:
                count = self.attempts_by_key.get(idem_key, 0) + 1
                self.attempts_by_key[idem_key] = count
            if count == 1:
                self.send_response(503)
                self.end_headers()
                return
            with self.state_lock:
                self.committed_orders[idem_key] = {"order_id": "ORD-992", "commits": 1}
                record = self.committed_orders[idem_key]
            body = json.dumps(record).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)


def test_real_http_timeouts_lost_acks_duplicates_and_partial_failures():
    AdversarialFaultHandler.attempts_by_key.clear()
    AdversarialFaultHandler.committed_orders.clear()

    server = ThreadingHTTPServer(("127.0.0.1", 0), AdversarialFaultHandler)
    port = server.server_address[1]
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()

    try:
        base_url = f"http://127.0.0.1:{port}"
        store = InMemoryCausalStore()
        rt = ContinuityRuntime(store=store, run_id="http-fault-run")
        bridge = HttpBridge(runtime=rt, timeout=0.15)

        try:
            bridge.request(
                step_id="create_irreversible_order",
                method="POST",
                url=f"{base_url}/orders/lost-ack",
                payload={"item": "gpu-node"},
                side_effect=SideEffectClass.IRREVERSIBLE,
                reconcile_url=f"{base_url}/reconcile",
            )
            assert False, "Expected timeout on lost acknowledgement"
        except (TimeoutError, socket.timeout, OSError):
            pass

        res_a = bridge.request(
            step_id="create_irreversible_order",
            method="POST",
            url=f"{base_url}/orders/lost-ack",
            payload={"item": "gpu-node"},
            side_effect=SideEffectClass.IRREVERSIBLE,
            reconcile_url=f"{base_url}/reconcile",
        )
        assert res_a["status"] == 200
        assert res_a["body"]["order_id"] == "ORD-991"
        assert res_a["body"]["commits"] == 1

        res_dup = bridge.request(
            step_id="create_irreversible_order",
            method="POST",
            url=f"{base_url}/orders/lost-ack",
            payload={"item": "gpu-node"},
            side_effect=SideEffectClass.IRREVERSIBLE,
            reconcile_url=f"{base_url}/reconcile",
        )
        assert res_dup == res_a

        try:
            bridge.request(
                step_id="reserve_slot",
                method="POST",
                url=f"{base_url}/orders/partial-failure",
                payload={"slot": "A1"},
                side_effect=SideEffectClass.IDEMPOTENT,
            )
            assert False, "Expected 503 HTTPError on first attempt"
        except Exception:
            pass

        res_b = bridge.request(
            step_id="reserve_slot",
            method="POST",
            url=f"{base_url}/orders/partial-failure",
            payload={"slot": "A1"},
            side_effect=SideEffectClass.IDEMPOTENT,
        )
        assert res_b["status"] == 200
        assert res_b["body"]["order_id"] == "ORD-992"

        report = InvariantChecker().check_run("http-fault-run", store.get_events("http-fault-run"))
        assert report.passed, f"Invariant violations: {report.violations}"
    finally:
        server.shutdown()
        server.server_close()
