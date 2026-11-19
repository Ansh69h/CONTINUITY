import asyncio
import inspect
from typing import Any, Callable, Optional

from continuity import ContinuityRuntime, CausalStoreProtocol


def create_status_router(
    store: CausalStoreProtocol,
    *,
    authorize: Callable[..., Any],
    resume_handler: Optional[Callable[[str, str], Any]] = None,
    prefix: str = "/continuity",
    include_payload: bool = False,
) -> Any:
    """Build authorized run-inspection and optional application-owned resume routes.

    A resume handler must call the application's registered workflow through a
    ``ContinuityRuntime``. The adapter does not infer or dynamically execute
    workflow code from a request.
    """
    try:
        from fastapi import APIRouter, HTTPException, Request
    except ImportError as exc:
        raise ImportError("Install the FastAPI extra with `pip install '.[fastapi]'`") from exc

    if not callable(authorize):
        raise TypeError("authorize must be a callable authentication/authorization hook")
    router = APIRouter(prefix=prefix, tags=["continuity"])

    async def check_access(request: Any, action: str, run_id: Optional[str] = None, step_id: Optional[str] = None) -> None:
        result = authorize(request, action=action, run_id=run_id, step_id=step_id)
        if inspect.isawaitable(result):
            result = await result
        if not result:
            raise HTTPException(status_code=403, detail="Not authorized for this CONTINUITY operation")

    def event_record(event: Any) -> dict[str, Any]:
        record = event.to_dict()
        if not include_payload:
            record.pop("payload", None)
            record.pop("idempotency_key", None)
        return record

    @router.get("/runs")
    async def list_runs(request: Request) -> dict[str, Any]:
        await check_access(request, "list_runs")
        return {"runs": store.list_runs()}

    @router.get("/runs/{run_id}")
    async def inspect_run(run_id: str, request: Request) -> dict[str, Any]:
        await check_access(request, "inspect_run", run_id)
        return {"run_id": run_id, "events": [event_record(event) for event in store.get_events(run_id)]}

    @router.get("/runs/{run_id}/recoveries")
    async def inspect_recoveries(run_id: str, request: Request) -> dict[str, Any]:
        await check_access(request, "inspect_recoveries", run_id)
        runtime = ContinuityRuntime(store=store, run_id=run_id)
        decisions = runtime.inspect_pending_recoveries()
        return {
            "run_id": run_id,
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

    @router.post("/runs/{run_id}/steps/{step_id}/resume")
    async def resume_step(run_id: str, step_id: str, request: Request) -> dict[str, Any]:
        await check_access(request, "resume_step", run_id, step_id)
        if resume_handler is None:
            raise HTTPException(status_code=501, detail="No application workflow resume handler is configured")
        if inspect.iscoroutinefunction(resume_handler):
            result = await resume_handler(run_id, step_id)
        else:
            result = await asyncio.to_thread(resume_handler, run_id, step_id)
        if inspect.isawaitable(result):
            await result
        return {"run_id": run_id, "step_id": step_id, "status": "resume_requested"}

    return router
