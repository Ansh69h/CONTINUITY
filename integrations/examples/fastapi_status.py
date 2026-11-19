"""Authorized status routes; wire app-specific auth and resume dispatch."""

import hmac
import os

from fastapi import FastAPI

from continuity import SQLiteCausalStore
from continuity_integrations.fastapi import create_status_router

store = SQLiteCausalStore("workflow.db")


async def authorize(request, *, action: str, run_id: str | None, step_id: str | None) -> bool:
    # Set this secret in the host environment; missing configuration denies access.
    expected_token = os.environ.get("CONTINUITY_ADMIN_TOKEN")
    supplied_token = request.headers.get("x-admin-token")
    return bool(expected_token and supplied_token and hmac.compare_digest(supplied_token, expected_token))


async def resume_registered_step(run_id: str, step_id: str) -> None:
    # Dispatch through an application-owned registry and its ContinuityRuntime.
    raise NotImplementedError("Connect this callback to the application's durable workflow registry")


app = FastAPI()
app.include_router(
    create_status_router(store, authorize=authorize, resume_handler=resume_registered_step)
)
