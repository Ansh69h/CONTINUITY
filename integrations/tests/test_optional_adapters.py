import asyncio
import runpy
from pathlib import Path
from types import SimpleNamespace

import pytest


def test_openai_agents_decorator_returns_sdk_function_tool_when_installed():
    pytest.importorskip("agents.decorators")
    from continuity import InMemoryCausalStore, ContinuityRuntime
    from continuity_integrations.openai_agents import durable_function_tool

    runtime = ContinuityRuntime(store=InMemoryCausalStore(), run_id="sdk-test")

    @durable_function_tool(runtime, step_id="multiply")
    def multiply(a: int, b: int) -> int:
        """Multiply two numbers."""
        return a * b

    assert multiply.name == "multiply"
    assert multiply.params_json_schema["type"] == "object"


def test_fastapi_status_router_requires_authorization_hook_when_installed():
    pytest.importorskip("fastapi")
    from continuity import InMemoryCausalStore
    from continuity_integrations.fastapi import create_status_router

    store = InMemoryCausalStore()
    router = create_status_router(store, authorize=lambda *_args, **_kwargs: True)

    paths = {route.path for route in router.routes}
    assert "/continuity/runs" in paths
    assert "/continuity/runs/{run_id}/recoveries" in paths
    assert "/continuity/runs/{run_id}/steps/{step_id}/resume" in paths


def test_fastapi_authorization_fails_closed_for_falsey_results_when_installed():
    pytest.importorskip("fastapi")
    pytest.importorskip("httpx")
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from continuity import InMemoryCausalStore
    from continuity_integrations.fastapi import create_status_router

    app = FastAPI()
    app.include_router(create_status_router(InMemoryCausalStore(), authorize=lambda *_args, **_kwargs: None))
    with TestClient(app) as client:
        assert client.get("/continuity/runs").status_code == 403


def test_langgraph_node_runs_with_public_stategraph_api_when_installed():
    pytest.importorskip("langgraph.graph")
    from typing import TypedDict

    from continuity import ContinuityRuntime, InMemoryCausalStore
    from continuity_integrations.langgraph import durable_node
    from langgraph.graph import END, START, StateGraph

    store = InMemoryCausalStore()
    runtimes = {}

    def runtime_for_thread(thread_id):
        return runtimes.setdefault(thread_id, ContinuityRuntime(store=store, run_id=f"graph:{thread_id}"))

    calls = []

    class State(TypedDict):
        value: int
        doubled: int

    @durable_node(runtime_for_thread, step_id="double")
    def double(state: State, config):
        calls.append(state["value"])
        return {"doubled": state["value"] * 2}

    graph = StateGraph(State)
    graph.add_node("double", double)
    graph.add_edge(START, "double")
    graph.add_edge("double", END)
    app = graph.compile()
    config = {"configurable": {"thread_id": "thread-1"}}

    first = app.invoke({"value": 7, "doubled": 0}, config=config)
    second = app.invoke({"value": 7, "doubled": 0}, config=config)

    assert first["doubled"] == second["doubled"] == 14
    assert calls == [7]


def test_langgraph_async_node_runs_with_public_stategraph_api_when_installed():
    pytest.importorskip("langgraph.graph")
    import asyncio
    from typing import TypedDict

    from continuity import ContinuityRuntime, InMemoryCausalStore
    from continuity_integrations.langgraph import durable_node
    from langgraph.graph import END, START, StateGraph

    store = InMemoryCausalStore()
    runtime = ContinuityRuntime(store=store, run_id="async-graph")

    class State(TypedDict):
        value: int
        doubled: int

    @durable_node(runtime, step_id="double-async")
    async def double_async(state: State, config):
        return {"doubled": state["value"] * 2}

    graph = StateGraph(State)
    graph.add_node("double_async", double_async)
    graph.add_edge(START, "double_async")
    graph.add_edge("double_async", END)
    app = graph.compile()
    result = asyncio.run(app.ainvoke({"value": 9, "doubled": 0}))

    assert result["doubled"] == 18


def test_fastapi_router_authorizes_and_dispatches_resume_when_installed():
    pytest.importorskip("fastapi")
    pytest.importorskip("httpx")
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from continuity import InMemoryCausalStore
    from continuity_integrations.fastapi import create_status_router

    calls = []
    store = InMemoryCausalStore()
    router = create_status_router(
        store,
        authorize=lambda request, **_kwargs: request.headers.get("x-admin") == "allowed",
        resume_handler=lambda run_id, step_id: calls.append((run_id, step_id)),
    )
    app = FastAPI()
    app.include_router(router)

    with TestClient(app) as client:
        assert client.get("/continuity/runs").status_code == 403
        assert client.get("/continuity/runs", headers={"x-admin": "allowed"}).json() == {"runs": []}
        response = client.post(
            "/continuity/runs/demo/steps/reconcile/resume",
            headers={"x-admin": "allowed"},
        )

    assert response.status_code == 200
    assert calls == [("demo", "reconcile")]


def test_fastapi_status_example_fails_closed_without_configured_admin_token(tmp_path, monkeypatch):
    pytest.importorskip("fastapi")
    monkeypatch.delenv("CONTINUITY_ADMIN_TOKEN", raising=False)
    monkeypatch.chdir(tmp_path)
    example_path = Path(__file__).resolve().parents[1] / "examples" / "fastapi_status.py"
    authorize = runpy.run_path(str(example_path))["authorize"]

    request = SimpleNamespace(headers={"x-admin-token": "arbitrary"})
    assert asyncio.run(authorize(request, action="list_runs", run_id=None, step_id=None)) is False

    monkeypatch.setenv("CONTINUITY_ADMIN_TOKEN", "configured-secret")
    request.headers["x-admin-token"] = "configured-secret"
    assert asyncio.run(authorize(request, action="list_runs", run_id=None, step_id=None)) is True
