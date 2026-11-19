from __future__ import annotations

import hashlib
import json
import logging
import urllib.request
from collections import Counter, defaultdict
from typing import Any, Dict, List, Optional

from .models import CausalEvent, EventType


def _hex_id(seed: str, length: int) -> str:
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()[:length]


def _attribute(key: str, value: Any) -> Dict[str, Any]:
    if isinstance(value, bool):
        return {"key": key, "value": {"boolValue": value}}
    if isinstance(value, int):
        return {"key": key, "value": {"intValue": str(value)}}
    return {"key": key, "value": {"stringValue": str(value)}}


def build_otlp_payload(
    events: List[CausalEvent],
    service_name: str = "continuity-runtime",
    include_sensitive_attributes: bool = False,
) -> Dict[str, Any]:
    """Build OTLP JSON with one workflow span and measured step spans per attempt.

    Payload values and idempotency keys are never exported. Raw run/step identifiers and
    user-defined step names are included only when explicitly opted in. Deterministic
    trace/span IDs still correlate repeated exports of the same run by default.
    """
    by_run: Dict[str, List[CausalEvent]] = defaultdict(list)
    for event in events:
        by_run[event.run_id].append(event)

    resource_spans = []
    for run_id, run_events in sorted(by_run.items()):
        run_events.sort(key=lambda event: event.seq)
        trace_id = _hex_id(run_id, 32)
        workflow_span_id = _hex_id(f"workflow:{run_id}", 16)
        start_ns = int(min(event.timestamp for event in run_events) * 1_000_000_000)
        end_ns = max(start_ns, int(max(event.timestamp for event in run_events) * 1_000_000_000))
        workflow_attributes = [_attribute("continuity.event_count", len(run_events))]
        if include_sensitive_attributes:
            workflow_attributes.append(_attribute("continuity.run_id", run_id))
        spans = [
            {
                "traceId": trace_id,
                "spanId": workflow_span_id,
                "parentSpanId": "",
                "name": "continuity.run",
                "kind": 1,
                "startTimeUnixNano": str(start_ns),
                "endTimeUnixNano": str(end_ns),
                "attributes": workflow_attributes,
                "events": [],
            }
        ]

        attempts: Dict[tuple[str, int], List[CausalEvent]] = defaultdict(list)
        for event in run_events:
            attempts[(event.step_id, event.attempt)].append(event)
        for (step_id, attempt), step_events in sorted(
            attempts.items(), key=lambda pair: (min(event.seq for event in pair[1]), pair[0][0], pair[0][1])
        ):
            step_events.sort(key=lambda event: event.seq)
            starts = [e for e in step_events if e.event_type in (EventType.STEP_INTENT, EventType.STEP_STARTED)]
            terminals = [
                e for e in step_events
                if e.event_type in (EventType.STEP_COMMITTED, EventType.STEP_FAILED, EventType.COMPENSATION_COMMITTED)
            ]
            step_start = starts[0] if starts else step_events[0]
            step_end = terminals[-1] if terminals else step_events[-1]
            span_start_ns = int(step_start.timestamp * 1_000_000_000)
            span_end_ns = max(span_start_ns, int(step_end.timestamp * 1_000_000_000))
            step_span_id = _hex_id(f"{run_id}:{step_id}:{attempt}", 16)
            attributes = [
                _attribute("continuity.attempt", attempt),
                _attribute("continuity.side_effect", step_events[0].side_effect.value),
                _attribute("continuity.event_count", len(step_events)),
            ]
            if include_sensitive_attributes:
                attributes.extend(
                    [
                        _attribute("continuity.run_id", run_id),
                        _attribute("continuity.step_id", step_id),
                        _attribute("continuity.step_name", step_events[0].step_name),
                    ]
                )
            status = 0
            if any(e.event_type == EventType.STEP_COMMITTED for e in step_events):
                status = 1
            elif any(e.event_type == EventType.STEP_FAILED for e in step_events):
                status = 2
            spans.append(
                {
                    "traceId": trace_id,
                    "spanId": step_span_id,
                    "parentSpanId": workflow_span_id,
                    "name": step_events[0].step_name if include_sensitive_attributes else "continuity.step",
                    "kind": 1,
                    "startTimeUnixNano": str(span_start_ns),
                    "endTimeUnixNano": str(span_end_ns),
                    "attributes": attributes,
                    "events": [
                        {
                            "timeUnixNano": str(int(event.timestamp * 1_000_000_000)),
                            "name": event.event_type.value,
                            "attributes": [
                                _attribute("continuity.seq", event.seq),
                                _attribute("continuity.event_type", event.event_type.value),
                            ],
                        }
                        for event in step_events
                    ],
                    "status": {"code": status},
                }
            )
        resource_spans.append(
            {
                "resource": {"attributes": [_attribute("service.name", service_name)]},
                "scopeSpans": [{"scope": {"name": "continuity"}, "spans": spans}],
            }
        )
    return {"resourceSpans": resource_spans}


def build_runtime_metrics(events: List[CausalEvent]) -> Dict[str, Any]:
    """Return dependency-free low-cardinality counters and duration summaries."""
    event_counts = Counter(event.event_type.value for event in events)
    step_durations: List[float] = []
    groups: Dict[tuple[str, str, int], List[CausalEvent]] = defaultdict(list)
    for event in events:
        groups[(event.run_id, event.step_id, event.attempt)].append(event)
    for group in groups.values():
        started = [e.timestamp for e in group if e.event_type in (EventType.STEP_INTENT, EventType.STEP_STARTED)]
        ended = [
            e.timestamp for e in group
            if e.event_type in (EventType.STEP_COMMITTED, EventType.STEP_FAILED)
        ]
        if started and ended:
            step_durations.append(max(0.0, max(ended) - min(started)))
    by_side_effect = Counter(event.side_effect.value for event in events if event.event_type == EventType.STEP_STARTED)
    return {
        "events_total": dict(sorted(event_counts.items())),
        "steps_started_total": dict(sorted(by_side_effect.items())),
        "step_duration_seconds": {
            "count": len(step_durations),
            "sum": sum(step_durations),
            "max": max(step_durations, default=0.0),
        },
    }


def log_causal_event(
    logger: logging.Logger,
    event: CausalEvent,
    include_sensitive_attributes: bool = False,
) -> None:
    """Emit a structured standard-library log record without payload data by default."""
    attributes: Dict[str, Any] = {
        "event_type": event.event_type.value,
        "seq": event.seq,
        "side_effect": event.side_effect.value,
        "attempt": event.attempt,
    }
    if include_sensitive_attributes:
        attributes.update({"run_id": event.run_id, "step_id": event.step_id})
    logger.info(
        "continuity event",
        extra={"continuity": attributes},
    )


def export_otlp_traces(
    events: List[CausalEvent],
    endpoint: str = "http://localhost:4318/v1/traces",
    headers: Optional[Dict[str, str]] = None,
    timeout: float = 5.0,
    include_sensitive_attributes: bool = False,
) -> int:
    payload = build_otlp_payload(events, include_sensitive_attributes=include_sensitive_attributes)
    body = json.dumps(payload).encode("utf-8")
    req_headers = {"Content-Type": "application/json"}
    if headers:
        req_headers.update(headers)
    req = urllib.request.Request(endpoint, data=body, headers=req_headers, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return int(resp.status)
