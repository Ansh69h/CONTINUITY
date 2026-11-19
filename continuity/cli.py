from __future__ import annotations

import argparse
import importlib
import inspect
import json
from dataclasses import asdict
from typing import Any, Callable, Optional, Sequence

from .models import SideEffectClass, safe_json_default
from .policy import SideEffectPolicy
from .runtime import ContinuityRuntime, EscalationRequiredError
from .storage.base import SQLiteCausalStore
from .validation.invariants import InvariantChecker


def _load_object(spec: str) -> Any:
    if ":" not in spec:
        raise ValueError("Use module.path:object_name syntax")
    module_name, object_name = spec.split(":", 1)
    if not module_name or not object_name:
        raise ValueError("Use module.path:object_name syntax")
    obj: Any = importlib.import_module(module_name)
    for part in object_name.split("."):
        obj = getattr(obj, part)
    return obj


def _json(value: Any) -> str:
    return json.dumps(value, indent=2, sort_keys=True, default=safe_json_default)


def _event_for_inspection(event: Any, include_payload: bool) -> dict[str, Any]:
    data = event.to_dict()
    if include_payload:
        return data
    return {
        "seq": data["seq"],
        "event_type": data["event_type"],
        "side_effect": data["side_effect"],
        "attempt": data["attempt"],
        "timestamp": data["timestamp"],
        "schema_version": data["schema_version"],
        "step_name": "[redacted]",
        "step_id": "[redacted]",
        "event_id": "[redacted]",
        "idempotency_key": "[redacted]",
        "parent_event_ids": [],
        "worker_id": "[redacted]",
        "payload": "[redacted]",
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="continuity", description="Inspect and safely recover CONTINUITY workflows")
    commands = parser.add_subparsers(dest="resource", required=True)
    runs = commands.add_parser("runs", help="inspect durable workflow runs")
    actions = runs.add_subparsers(dest="action", required=True)

    actions.add_parser("list", help="list known run IDs").add_argument("--database", default="continuity.db")

    for action in ("inspect", "recoveries", "verify"):
        command = actions.add_parser(action)
        command.add_argument("--database", default="continuity.db")
        command.add_argument("--run-id", required=True)
        if action == "inspect":
            command.add_argument("--include-payload", action="store_true", help="show stored step inputs and outputs")

    resume = actions.add_parser("resume", help="run an application handler through CONTINUITY recovery checks")
    resume.add_argument("--database", default="continuity.db")
    resume.add_argument("--run-id", required=True)
    resume.add_argument("--step-id", required=True)
    resume.add_argument("--handler", required=True, help="application callable as module.path:function")
    resume.add_argument("--args-json", default="[]", help="JSON array of positional arguments")
    resume.add_argument("--kwargs-json", default="{}", help="JSON object of keyword arguments")
    resume.add_argument("--side-effect", choices=[item.value for item in SideEffectClass], default="PURE")
    resume.add_argument("--step-name")
    resume.add_argument("--approve", action="store_true", help="record an authorized one-attempt recovery approval")
    resume.add_argument("--authorizer", help="authorization callable as module.path:function")
    resume.add_argument("--reason", help="required when --approve is used")
    return parser


def _run(args: argparse.Namespace) -> int:
    store = SQLiteCausalStore(args.database)
    if args.action == "list":
        print(_json({"runs": store.list_runs()}))
        return 0

    if args.action == "inspect":
        events = store.get_events(args.run_id)
        print(_json({"run_id": args.run_id, "events": [_event_for_inspection(e, args.include_payload) for e in events]}))
        return 0

    if args.action == "recoveries":
        runtime = ContinuityRuntime(store=store, run_id=args.run_id)
        decisions = runtime.inspect_pending_recoveries()
        print(
            _json(
                {
                    "run_id": args.run_id,
                    "recoveries": [
                        {
                            "step_id": decision.step_id,
                            "step_name": decision.step_name,
                            "action": decision.action.value,
                            "reason": decision.reason,
                            "attempt": decision.attempt,
                        }
                        for decision in decisions
                    ],
                }
            )
        )
        return 0

    if args.action == "verify":
        events = store.get_events(args.run_id)
        report = InvariantChecker().check_run(args.run_id, events)
        print(
            _json(
                {
                    "run_id": report.run_id,
                    "passed": report.passed,
                    "event_count": report.event_count,
                    "checked_invariants": report.checked_invariants,
                    "violations": [asdict(item) for item in report.violations],
                }
            )
        )
        return 0 if report.passed else 1

    if args.action == "resume":
        try:
            handler = _load_object(args.handler)
            if not callable(handler):
                raise TypeError("--handler must resolve to a callable")
            if inspect.iscoroutinefunction(handler):
                raise TypeError("async handlers are not supported by CLI resume; provide a synchronous handler")
            step_args = json.loads(args.args_json)
            step_kwargs = json.loads(args.kwargs_json)
            if not isinstance(step_args, list) or not isinstance(step_kwargs, dict):
                raise ValueError("--args-json must be a JSON array and --kwargs-json must be a JSON object")
            if args.approve and (not args.authorizer or not args.reason):
                raise ValueError("--approve requires both --authorizer and --reason")
            if args.authorizer and not args.approve:
                raise ValueError("--authorizer requires --approve")
            authorizer: Optional[Callable[..., Any]] = _load_object(args.authorizer) if args.authorizer else None
            if authorizer is not None and not callable(authorizer):
                raise TypeError("--authorizer must resolve to a callable")
            side_effect = SideEffectClass(args.side_effect)
            policy = SideEffectPolicy(side_effect, allow_operator_approval=args.approve)
            runtime = ContinuityRuntime(
                store=store,
                run_id=args.run_id,
                operator_authorizer=authorizer,
            )
            if args.approve:
                runtime.approve_resume(
                    args.step_id,
                    *step_args,
                    policy=policy,
                    reason=args.reason,
                    step_name=args.step_name,
                    authorization_context={"source": "continuity-cli"},
                    **step_kwargs,
                )
            result = runtime.step(
                handler,
                *step_args,
                step_id=args.step_id,
                step_name=args.step_name,
                policy=policy,
                **step_kwargs,
            )
            print(_json({"run_id": args.run_id, "step_id": args.step_id, "result": result}))
            return 0
        except EscalationRequiredError as exc:
            print(_json({"error": "recovery_escalated", "step_id": exc.decision.step_id, "reason": exc.decision.reason}))
            return 3
        except Exception as exc:
            print(_json({"error": type(exc).__name__, "message": str(exc)}))
            return 1

    raise AssertionError(f"Unhandled command {args.action}")


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    try:
        return _run(args)
    except Exception as exc:
        print(_json({"error": type(exc).__name__, "message": str(exc)}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
