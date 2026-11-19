from continuity import CausalEvent, ContinuityRuntime, EventType, InMemoryCausalStore, SideEffectClass, build_otlp_payload


def test_telemetry_uses_measured_step_spans_and_redacts_identifiers_by_default():
    store = InMemoryCausalStore()
    runtime = ContinuityRuntime(store=store, run_id="private-run-id")
    runtime.step(lambda secret: {"token": secret}, "private-input", step_id="private-step")

    payload = build_otlp_payload(store.get_events("private-run-id"))
    spans = payload["resourceSpans"][0]["scopeSpans"][0]["spans"]

    assert len(spans) == 2  # workflow root and one step span
    workflow, step = spans
    assert workflow["name"] == "continuity.run"
    assert step["name"] == "continuity.step"
    assert step["parentSpanId"] == workflow["spanId"]
    assert int(step["endTimeUnixNano"]) >= int(step["startTimeUnixNano"])
    serialized = str(payload)
    assert "private-run-id" not in serialized
    assert "private-input" not in serialized
    assert "idempotency_key" not in serialized


def test_telemetry_opt_in_still_never_exports_idempotency_keys():
    event = CausalEvent(
        run_id="run-secret",
        seq=1,
        event_id="event-1",
        step_id="step-secret",
        step_name="do_work",
        event_type=EventType.STEP_INTENT,
        side_effect=SideEffectClass.PURE,
        idempotency_key="key-secret",
        timestamp=1.0,
    )
    payload = build_otlp_payload([event], include_sensitive_attributes=True)

    assert "run-secret" in str(payload)
    assert "step-secret" in str(payload)
    assert "key-secret" not in str(payload)
    assert "continuity.idempotency_key" not in str(payload)
