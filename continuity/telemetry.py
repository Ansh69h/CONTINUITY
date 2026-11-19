from __future__ import annotations
import hashlib
import json
import urllib.request
from typing import Any, Dict, List, Optional
from .models import CausalEvent


def _hex_id(seed: str, length: int) -> str:
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()[:length]


def build_otlp_payload(events: List[CausalEvent], service_name: str = "continuity-runtime") -> Dict[str, Any]:
    spans = []
    for ev in events:
        trace_id = _hex_id(ev.run_id, 32)
        span_id = _hex_id(ev.event_id, 16)
        parent_span_id = _hex_id(ev.parent_event_ids[-1], 16) if ev.parent_event_ids else ""
        start_ns = int(ev.timestamp * 1_000_000_000)
        spans.append(
            {
                "traceId": trace_id,
                "spanId": span_id,
                "parentSpanId": parent_span_id,
                "name": f"{ev.step_name}:{ev.event_type.value}",
                "kind": 1,
                "startTimeUnixNano": str(start_ns),
                "endTimeUnixNano": str(start_ns + 1_000),
                "attributes": [
                    {"key": "continuity.run_id", "value": {"stringValue": ev.run_id}},
                    {"key": "continuity.step_id", "value": {"stringValue": ev.step_id}},
                    {"key": "continuity.seq", "value": {"intValue": str(ev.seq)}},
                    {"key": "continuity.side_effect", "value": {"stringValue": ev.side_effect.value}},
                    {"key": "continuity.idempotency_key", "value": {"stringValue": ev.idempotency_key}},
                ],
            }
        )
    return {
        "resourceSpans": [
            {
                "resource": {
                    "attributes": [{"key": "service.name", "value": {"stringValue": service_name}}]
                },
                "scopeSpans": [{"scope": {"name": "continuity"}, "spans": spans}],
            }
        ]
    }


def export_otlp_traces(
    events: List[CausalEvent],
    endpoint: str = "http://localhost:4318/v1/traces",
    headers: Optional[Dict[str, str]] = None,
    timeout: float = 5.0,
) -> int:
    payload = build_otlp_payload(events)
    body = json.dumps(payload).encode("utf-8")
    req_headers = {"Content-Type": "application/json"}
    if headers:
        req_headers.update(headers)
    req = urllib.request.Request(endpoint, data=body, headers=req_headers, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return int(resp.status)
