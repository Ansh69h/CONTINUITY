from __future__ import annotations
import asyncio
import functools
import hashlib
import inspect
import json
import threading
import time
import urllib.error
import urllib.request
import uuid
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple
from .models import (
    CausalEvent,
    EventType,
    SideEffectClass,
    RecoveryAction,
    RecoveryDecision,
    safe_json_default,
)
from .planner import RecoveryPlanner, AsyncRecoveryPlanner
from .storage.base import CausalStoreProtocol, InMemoryCausalStore, OptimisticConcurrencyError


class EscalationRequiredError(RuntimeError):
    """Raised when causal safety rules forbid automatic retry and require human/operator escalation."""

    def __init__(self, decision: RecoveryDecision) -> None:
        self.decision = decision
        super().__init__(f"[{decision.step_id}] {decision.reason}")


class LeaseContentionError(RuntimeError):
    """Raised when another worker holds an active lease on the target step."""


class _LeaseRenewer:
    """Refresh a step lease while its callable or recovery hook is running."""

    def __init__(self, store: CausalStoreProtocol, run_id: str, step_id: str, lease_owner: str, ttl: float) -> None:
        self._store = store
        self._run_id = run_id
        self._step_id = step_id
        self._lease_owner = lease_owner
        self._ttl = ttl
        self._interval = max(min(ttl / 3.0, 1.0), 0.001)
        self._stop_event = threading.Event()
        self._lost_event = threading.Event()
        self._thread = threading.Thread(target=self._renew, name=f"continuity-lease-{step_id}", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def _renew(self) -> None:
        while not self._stop_event.wait(self._interval):
            try:
                renewed = self._store.acquire_step_lease(
                    self._run_id,
                    self._step_id,
                    self._lease_owner,
                    ttl_seconds=self._ttl,
                )
            except Exception:
                self._lost_event.set()
                return
            if not renewed:
                self._lost_event.set()
                return

    def assert_held(self) -> None:
        if self._lost_event.is_set():
            raise LeaseContentionError(
                f"Lease for step {self._step_id} in run {self._run_id} was lost during execution"
            )

    def stop(self) -> None:
        self._stop_event.set()
        self._thread.join()


def _compute_idempotency_key(run_id: str, step_id: str, args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> str:
    canonical = json.dumps(
        {"run_id": run_id, "step_id": step_id, "args": args, "kwargs": kwargs},
        sort_keys=True,
        default=safe_json_default,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:24]


class ContinuityRuntime:
    def __init__(
        self,
        store: Optional[CausalStoreProtocol] = None,
        run_id: Optional[str] = None,
        worker_id: Optional[str] = None,
        lease_ttl: float = 30.0,
        max_attempts: int = 3,
    ) -> None:
        if lease_ttl <= 0:
            raise ValueError("lease_ttl must be positive")
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        self.store: CausalStoreProtocol = store or InMemoryCausalStore()
        self.run_id = run_id or f"run-{uuid.uuid4().hex[:12]}"
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self.lease_ttl = lease_ttl
        self.max_attempts = max_attempts
        self.planner = RecoveryPlanner()
        self.async_planner = AsyncRecoveryPlanner()

    def _validate_step_history(
        self,
        events: List[CausalEvent],
        step_id: str,
        step_name: str,
        side_effect: SideEffectClass,
        idempotency_key: str,
    ) -> None:
        prior_events = [event for event in events if event.step_id == step_id]
        if not prior_events:
            return

        reason = None
        if any(event.idempotency_key != idempotency_key for event in prior_events):
            reason = (
                f"Step {step_id} already has causal history for different inputs; "
                "use a new step_id or run_id."
            )
        elif any(event.side_effect != side_effect for event in prior_events):
            reason = (
                f"Step {step_id} was previously recorded as a different side-effect class; "
                "use a new step_id or run_id."
            )

        if reason:
            latest_attempt = max(event.attempt for event in prior_events)
            raise EscalationRequiredError(
                RecoveryDecision(
                    step_id=step_id,
                    step_name=step_name,
                    action=RecoveryAction.HALT_ESCALATE,
                    reason=reason,
                    attempt=latest_attempt + 1,
                )
            )

    def _append_with_retry(
        self,
        step_id: str,
        step_name: str,
        event_type: EventType,
        side_effect: SideEffectClass,
        idempotency_key: str,
        payload: Optional[Dict[str, Any]] = None,
        attempt: int = 1,
        max_cas_retries: int = 16,
    ) -> CausalEvent:
        for _ in range(max_cas_retries):
            existing = self.store.get_events(self.run_id)
            next_seq = len(existing) + 1
            parents = [existing[-1].event_id] if existing else []
            ev = CausalEvent(
                run_id=self.run_id,
                seq=next_seq,
                event_id=f"evt-{uuid.uuid4().hex[:16]}",
                step_id=step_id,
                step_name=step_name,
                event_type=event_type,
                side_effect=side_effect,
                idempotency_key=idempotency_key,
                parent_event_ids=parents,
                payload=payload or {},
                timestamp=time.time(),
                worker_id=self.worker_id,
                attempt=attempt,
            )
            try:
                return self.store.append_event(ev)
            except OptimisticConcurrencyError:
                time.sleep(0.001)
        raise OptimisticConcurrencyError(f"Failed CAS append after {max_cas_retries} retries on run {self.run_id}")

    def step(
        self,
        fn: Callable[..., Any],
        *args: Any,
        step_id: Optional[str] = None,
        step_name: Optional[str] = None,
        side_effect: SideEffectClass = SideEffectClass.PURE,
        reconciler: Optional[Callable[[str], Tuple[bool, Any]]] = None,
        compensator: Optional[Callable[[str], Any]] = None,
        before_commit_hook: Optional[Callable[[], None]] = None,
        after_start_hook: Optional[Callable[[], None]] = None,
        **kwargs: Any,
    ) -> Any:
        s_name = step_name or getattr(fn, "__name__", "anonymous_step")
        s_id = step_id or s_name
        idem_key = _compute_idempotency_key(self.run_id, s_id, args, kwargs)

        events = self.store.get_events(self.run_id)
        self._validate_step_history(events, s_id, s_name, side_effect, idem_key)
        decision = self.planner.analyze_step(
            events=events,
            step_id=s_id,
            step_name=s_name,
            side_effect=side_effect,
            has_reconciler=reconciler is not None,
            has_compensator=compensator is not None,
            max_attempts=self.max_attempts,
        )
        if decision.action == RecoveryAction.REUSE_CACHED:
            return decision.cached_output

        lease_owner = f"{self.worker_id}:{uuid.uuid4().hex}"
        if not self.store.acquire_step_lease(self.run_id, s_id, lease_owner, ttl_seconds=self.lease_ttl):
            raise LeaseContentionError(f"Step {s_id} in run {self.run_id} is locked by another worker")

        lease_renewer = _LeaseRenewer(self.store, self.run_id, s_id, lease_owner, self.lease_ttl)
        lease_renewer.start()
        try:
            events = self.store.get_events(self.run_id)
            self._validate_step_history(events, s_id, s_name, side_effect, idem_key)
            decision = self.planner.analyze_step(
                events=events,
                step_id=s_id,
                step_name=s_name,
                side_effect=side_effect,
                has_reconciler=reconciler is not None,
                has_compensator=compensator is not None,
                max_attempts=self.max_attempts,
            )

            if decision.action == RecoveryAction.REUSE_CACHED:
                return decision.cached_output

            if len(events) > 0 and any(e.step_id == s_id for e in events):
                self._append_with_retry(
                    step_id=s_id,
                    step_name=s_name,
                    event_type=EventType.RECOVERY_DECISION,
                    side_effect=side_effect,
                    idempotency_key=idem_key,
                    payload={"action": decision.action.value, "reason": decision.reason},
                    attempt=decision.attempt,
                )

            if decision.action == RecoveryAction.HALT_ESCALATE:
                raise EscalationRequiredError(decision)

            if decision.action == RecoveryAction.RECONCILE_EXTERNAL:
                assert reconciler is not None
                lease_renewer.assert_held()
                found, external_val = reconciler(idem_key)
                lease_renewer.assert_held()
                if found:
                    self._append_with_retry(
                        step_id=s_id,
                        step_name=s_name,
                        event_type=EventType.STEP_COMMITTED,
                        side_effect=side_effect,
                        idempotency_key=idem_key,
                        payload={"output": external_val, "reconciled": True},
                        attempt=decision.attempt,
                    )
                    return external_val

            if decision.action == RecoveryAction.COMPENSATE_AND_RETRY:
                assert compensator is not None
                lease_renewer.assert_held()
                self._append_with_retry(
                    step_id=s_id,
                    step_name=s_name,
                    event_type=EventType.COMPENSATION_INTENT,
                    side_effect=side_effect,
                    idempotency_key=idem_key,
                    payload={},
                    attempt=decision.attempt,
                )
                comp_res = compensator(idem_key)
                lease_renewer.assert_held()
                self._append_with_retry(
                    step_id=s_id,
                    step_name=s_name,
                    event_type=EventType.COMPENSATION_COMMITTED,
                    side_effect=side_effect,
                    idempotency_key=idem_key,
                    payload={"compensation_result": comp_res},
                    attempt=decision.attempt,
                )

            self._append_with_retry(
                step_id=s_id,
                step_name=s_name,
                event_type=EventType.STEP_INTENT,
                side_effect=side_effect,
                idempotency_key=idem_key,
                payload={"args": list(args), "kwargs": kwargs},
                attempt=decision.attempt,
            )
            self._append_with_retry(
                step_id=s_id,
                step_name=s_name,
                event_type=EventType.STEP_STARTED,
                side_effect=side_effect,
                idempotency_key=idem_key,
                payload={},
                attempt=decision.attempt,
            )
            if after_start_hook is not None:
                after_start_hook()
            lease_renewer.assert_held()

            try:
                lease_renewer.assert_held()
                result = fn(*args, **kwargs)
                lease_renewer.assert_held()
            except Exception as exc:
                self._append_with_retry(
                    step_id=s_id,
                    step_name=s_name,
                    event_type=EventType.STEP_FAILED,
                    side_effect=side_effect,
                    idempotency_key=idem_key,
                    payload={"error": f"{type(exc).__name__}: {exc}"},
                    attempt=decision.attempt,
                )
                raise

            if before_commit_hook is not None:
                before_commit_hook()
            lease_renewer.assert_held()

            self._append_with_retry(
                step_id=s_id,
                step_name=s_name,
                event_type=EventType.STEP_COMMITTED,
                side_effect=side_effect,
                idempotency_key=idem_key,
                payload={"output": result},
                attempt=decision.attempt,
            )
            return result
        finally:
            lease_renewer.stop()
            self.store.release_step_lease(self.run_id, s_id, lease_owner)

    async def step_async(
        self,
        fn: Callable[..., Awaitable[Any]],
        *args: Any,
        step_id: Optional[str] = None,
        step_name: Optional[str] = None,
        side_effect: SideEffectClass = SideEffectClass.PURE,
        reconciler: Optional[Callable[[str], Any]] = None,
        compensator: Optional[Callable[[str], Any]] = None,
        **kwargs: Any,
    ) -> Any:
        s_name = step_name or getattr(fn, "__name__", "anonymous_async_step")
        s_id = step_id or s_name
        idem_key = _compute_idempotency_key(self.run_id, s_id, args, kwargs)

        events = self.store.get_events(self.run_id)
        self._validate_step_history(events, s_id, s_name, side_effect, idem_key)
        decision = await self.async_planner.analyze_step_async(
            events=events,
            step_id=s_id,
            step_name=s_name,
            side_effect=side_effect,
            has_reconciler=reconciler is not None,
            has_compensator=compensator is not None,
            max_attempts=self.max_attempts,
        )
        if decision.action == RecoveryAction.REUSE_CACHED:
            return decision.cached_output

        lease_owner = f"{self.worker_id}:{uuid.uuid4().hex}"
        if not self.store.acquire_step_lease(self.run_id, s_id, lease_owner, ttl_seconds=self.lease_ttl):
            raise LeaseContentionError(f"Step {s_id} in run {self.run_id} is locked by another worker")

        lease_renewer = _LeaseRenewer(self.store, self.run_id, s_id, lease_owner, self.lease_ttl)
        lease_renewer.start()
        try:
            events = self.store.get_events(self.run_id)
            self._validate_step_history(events, s_id, s_name, side_effect, idem_key)
            decision = await self.async_planner.analyze_step_async(
                events=events,
                step_id=s_id,
                step_name=s_name,
                side_effect=side_effect,
                has_reconciler=reconciler is not None,
                has_compensator=compensator is not None,
                max_attempts=self.max_attempts,
            )
            if decision.action == RecoveryAction.REUSE_CACHED:
                return decision.cached_output

            if len(events) > 0 and any(e.step_id == s_id for e in events):
                self._append_with_retry(
                    step_id=s_id,
                    step_name=s_name,
                    event_type=EventType.RECOVERY_DECISION,
                    side_effect=side_effect,
                    idempotency_key=idem_key,
                    payload={"action": decision.action.value, "reason": decision.reason},
                    attempt=decision.attempt,
                )

            if decision.action == RecoveryAction.HALT_ESCALATE:
                raise EscalationRequiredError(decision)

            if decision.action == RecoveryAction.RECONCILE_EXTERNAL:
                assert reconciler is not None
                lease_renewer.assert_held()
                rec_out = reconciler(idem_key)
                if inspect.isawaitable(rec_out):
                    rec_out = await rec_out
                lease_renewer.assert_held()
                found, external_val = rec_out
                if found:
                    self._append_with_retry(
                        step_id=s_id,
                        step_name=s_name,
                        event_type=EventType.STEP_COMMITTED,
                        side_effect=side_effect,
                        idempotency_key=idem_key,
                        payload={"output": external_val, "reconciled": True},
                        attempt=decision.attempt,
                    )
                    return external_val

            if decision.action == RecoveryAction.COMPENSATE_AND_RETRY:
                assert compensator is not None
                lease_renewer.assert_held()
                self._append_with_retry(
                    step_id=s_id,
                    step_name=s_name,
                    event_type=EventType.COMPENSATION_INTENT,
                    side_effect=side_effect,
                    idempotency_key=idem_key,
                    payload={},
                    attempt=decision.attempt,
                )
                comp_res = compensator(idem_key)
                if inspect.isawaitable(comp_res):
                    comp_res = await comp_res
                lease_renewer.assert_held()
                self._append_with_retry(
                    step_id=s_id,
                    step_name=s_name,
                    event_type=EventType.COMPENSATION_COMMITTED,
                    side_effect=side_effect,
                    idempotency_key=idem_key,
                    payload={"compensation_result": comp_res},
                    attempt=decision.attempt,
                )

            lease_renewer.assert_held()
            self._append_with_retry(
                step_id=s_id,
                step_name=s_name,
                event_type=EventType.STEP_INTENT,
                side_effect=side_effect,
                idempotency_key=idem_key,
                payload={"args": list(args), "kwargs": kwargs},
                attempt=decision.attempt,
            )
            self._append_with_retry(
                step_id=s_id,
                step_name=s_name,
                event_type=EventType.STEP_STARTED,
                side_effect=side_effect,
                idempotency_key=idem_key,
                payload={},
                attempt=decision.attempt,
            )

            try:
                lease_renewer.assert_held()
                result = await fn(*args, **kwargs)
                lease_renewer.assert_held()
            except Exception as exc:
                self._append_with_retry(
                    step_id=s_id,
                    step_name=s_name,
                    event_type=EventType.STEP_FAILED,
                    side_effect=side_effect,
                    idempotency_key=idem_key,
                    payload={"error": f"{type(exc).__name__}: {exc}"},
                    attempt=decision.attempt,
                )
                raise

            self._append_with_retry(
                step_id=s_id,
                step_name=s_name,
                event_type=EventType.STEP_COMMITTED,
                side_effect=side_effect,
                idempotency_key=idem_key,
                payload={"output": result},
                attempt=decision.attempt,
            )
            return result
        finally:
            lease_renewer.stop()
            self.store.release_step_lease(self.run_id, s_id, lease_owner)

    def inspect_pending_recoveries(self) -> List[RecoveryDecision]:
        events = self.store.get_events(self.run_id)
        seen: Dict[str, CausalEvent] = {}
        for ev in events:
            seen[ev.step_id] = ev
        decisions: List[RecoveryDecision] = []
        for s_id, last_ev in seen.items():
            decisions.append(
                self.planner.analyze_step(
                    events=events,
                    step_id=s_id,
                    step_name=last_ev.step_name,
                    side_effect=last_ev.side_effect,
                    max_attempts=self.max_attempts,
                )
            )
        return decisions


def recoverable(
    runtime: ContinuityRuntime,
    side_effect: SideEffectClass = SideEffectClass.PURE,
    step_id: Optional[str] = None,
    reconciler: Optional[Callable[[str], Tuple[bool, Any]]] = None,
    compensator: Optional[Callable[[str], Any]] = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return runtime.step(
                fn,
                *args,
                step_id=step_id or fn.__name__,
                step_name=fn.__name__,
                side_effect=side_effect,
                reconciler=reconciler,
                compensator=compensator,
                **kwargs,
            )
        return wrapper
    return decorator


def recoverable_async(
    runtime: ContinuityRuntime,
    side_effect: SideEffectClass = SideEffectClass.PURE,
    step_id: Optional[str] = None,
    reconciler: Optional[Callable[[str], Any]] = None,
    compensator: Optional[Callable[[str], Any]] = None,
) -> Callable[[Callable[..., Awaitable[Any]]], Callable[..., Awaitable[Any]]]:
    def decorator(fn: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
        @functools.wraps(fn)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            return await runtime.step_async(
                fn,
                *args,
                step_id=step_id or fn.__name__,
                step_name=fn.__name__,
                side_effect=side_effect,
                reconciler=reconciler,
                compensator=compensator,
                **kwargs,
            )
        return wrapper
    return decorator


class HttpBridge:
    """Idempotent HTTP execution bridge with automatic Idempotency-Key propagation and reconciliation."""

    def __init__(self, runtime: ContinuityRuntime, timeout: float = 2.0) -> None:
        self.runtime = runtime
        self.timeout = timeout

    def request(
        self,
        step_id: str,
        method: str,
        url: str,
        payload: Optional[Dict[str, Any]] = None,
        side_effect: SideEffectClass = SideEffectClass.IDEMPOTENT,
        reconcile_url: Optional[str] = None,
        compensate_url: Optional[str] = None,
    ) -> Dict[str, Any]:
        norm_method = method.upper()
        idem_key = _compute_idempotency_key(
            self.runtime.run_id,
            step_id,
            (norm_method, url),
            {"payload_data": payload},
        )

        def _do_http(m: str, u: str, payload_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
            body = json.dumps(payload_data, default=safe_json_default).encode("utf-8") if payload_data is not None else None
            headers = {
                "Content-Type": "application/json",
                "Idempotency-Key": idem_key,
                "X-Continuity-Run-Id": self.runtime.run_id,
                "X-Continuity-Step-Id": step_id,
            }
            req = urllib.request.Request(u, data=body, headers=headers, method=m)
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                    raw = resp.read().decode("utf-8")
                    return {"status": resp.status, "body": json.loads(raw) if raw else {}}
            except urllib.error.HTTPError as err:
                err.close()
                raise

        def _reconciler(key: str) -> Tuple[bool, Any]:
            if not reconcile_url:
                return (False, None)
            req = urllib.request.Request(
                f"{reconcile_url}?idempotency_key={key}",
                headers={"Idempotency-Key": key},
                method="GET",
            )
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                    if resp.status == 200:
                        raw = resp.read().decode("utf-8")
                        return (True, {"status": 200, "body": json.loads(raw) if raw else {}, "reconciled": True})
                    return (False, None)
            except urllib.error.HTTPError as err:
                try:
                    if err.code == 404:
                        return (False, None)
                    raise
                finally:
                    err.close()

        def _compensator(key: str) -> Any:
            if not compensate_url:
                return None
            req = urllib.request.Request(
                compensate_url,
                data=json.dumps({"idempotency_key": key}).encode("utf-8"),
                headers={"Content-Type": "application/json", "Idempotency-Key": key},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return {"status": resp.status}

        return self.runtime.step(
            _do_http,
            norm_method,
            url,
            payload_data=payload,
            step_id=step_id,
            step_name=f"http_{method.lower()}",
            side_effect=side_effect,
            reconciler=_reconciler if reconcile_url else None,
            compensator=_compensator if compensate_url else None,
        )


class AsyncHttpBridge:
    def __init__(self, runtime: ContinuityRuntime, timeout: float = 2.0) -> None:
        self._sync_bridge = HttpBridge(runtime=runtime, timeout=timeout)
        self.runtime = runtime

    async def request(
        self,
        step_id: str,
        method: str,
        url: str,
        payload: Optional[Dict[str, Any]] = None,
        side_effect: SideEffectClass = SideEffectClass.IDEMPOTENT,
        reconcile_url: Optional[str] = None,
        compensate_url: Optional[str] = None,
    ) -> Dict[str, Any]:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            lambda: self._sync_bridge.request(
                step_id=step_id,
                method=method,
                url=url,
                payload=payload,
                side_effect=side_effect,
                reconcile_url=reconcile_url,
                compensate_url=compensate_url,
            ),
        )
