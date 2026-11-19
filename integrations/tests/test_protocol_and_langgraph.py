import asyncio

from continuity import SideEffectClass
from continuity_integrations.langgraph import durable_node
from continuity_integrations.protocol import durable_callable


class FakeRuntime:
    def __init__(self, run_id="test-run"):
        self.run_id = run_id
        self.calls = []

    def step(self, fn, *args, **kwargs):
        self.calls.append((kwargs["step_id"], args, kwargs))
        return fn(*args, **{k: v for k, v in kwargs.items() if k not in {"step_id", "step_name", "side_effect", "reconciler", "compensator"}})

    async def step_async(self, fn, *args, **kwargs):
        self.calls.append((kwargs["step_id"], args, kwargs))
        result = fn(*args, **{k: v for k, v in kwargs.items() if k not in {"step_id", "step_name", "side_effect", "reconciler", "compensator"}})
        return await result


def test_durable_callable_routes_sync_call_through_public_runtime():
    runtime = FakeRuntime()

    @durable_callable(runtime, step_id="normalize", side_effect=SideEffectClass.PURE)
    def normalize(value):
        return value.strip().lower()

    assert normalize("  HELLO ") == "hello"
    assert runtime.calls[0][0] == "normalize"


def test_durable_callable_routes_async_call_through_public_runtime():
    runtime = FakeRuntime()

    @durable_callable(runtime, step_id="fetch", side_effect=SideEffectClass.IDEMPOTENT)
    async def fetch(value):
        return f"got:{value}"

    assert asyncio.run(fetch("x")) == "got:x"
    assert runtime.calls[0][0] == "fetch"


def test_durable_callable_preserves_business_keyword_matching_runtime_control():
    runtime = FakeRuntime()

    @durable_callable(runtime, step_id="lookup-step")
    def lookup(*, step_id):
        return step_id

    assert lookup(step_id="business-value") == "business-value"


def test_async_durable_callable_preserves_business_keyword_matching_runtime_control():
    runtime = FakeRuntime()

    @durable_callable(runtime, step_id="lookup-step")
    async def lookup(*, step_id):
        return step_id

    assert asyncio.run(lookup(step_id="business-value")) == "business-value"


def test_langgraph_node_forwards_config_without_hashing_it_as_node_input():
    runtime = FakeRuntime()

    @durable_node(runtime, step_id="summarize")
    def node(state, config):
        return {"summary": state["text"], "thread": config["configurable"]["thread_id"]}

    config = {"configurable": {"thread_id": "thread-123"}, "callbacks": [object()]}
    result = node({"text": "hello"}, config=config)

    assert result == {"summary": "hello", "thread": "thread-123"}
    assert runtime.calls[0][1][0] == (({"text": "hello"},), {})
    assert "config" not in runtime.calls[0][2]


def test_langgraph_runtime_factory_uses_stable_thread_id():
    runtimes = {}

    def factory(run_id):
        runtimes.setdefault(run_id, FakeRuntime(run_id))
        return runtimes[run_id]

    @durable_node(factory, step_id="lookup")
    def node(state, config):
        return state

    result = node({"q": "hello"}, config={"configurable": {"thread_id": "thread-abc"}})

    assert result == {"q": "hello"}
    assert list(runtimes) == ["thread-abc"]
    assert runtimes["thread-abc"].calls[0][0] == "lookup"


def test_langgraph_runtime_factory_prefers_thread_id_over_trace_run_id():
    runtimes = {}

    def factory(run_id):
        runtimes.setdefault(run_id, FakeRuntime(run_id))
        return runtimes[run_id]

    @durable_node(factory, step_id="lookup")
    def node(state, config):
        return state

    result = node(
        {"q": "hello"},
        config={"run_id": "trace-a", "configurable": {"thread_id": "thread-1"}},
    )

    assert result == {"q": "hello"}
    assert list(runtimes) == ["thread-1"]


def test_langgraph_runtime_factory_extracts_positional_config():
    runtimes = {}

    def factory(run_id):
        runtimes.setdefault(run_id, FakeRuntime(run_id))
        return runtimes[run_id]

    @durable_node(factory, step_id="lookup")
    def node(state, config):
        return {**state, "thread": config["configurable"]["thread_id"]}

    result = node({"q": "hello"}, {"configurable": {"thread_id": "thread-positional"}})

    assert result == {"q": "hello", "thread": "thread-positional"}
    assert list(runtimes) == ["thread-positional"]


def test_langgraph_async_runtime_factory_extracts_positional_config():
    runtimes = {}

    def factory(run_id):
        runtimes.setdefault(run_id, FakeRuntime(run_id))
        return runtimes[run_id]

    @durable_node(factory, step_id="lookup-async")
    async def node(state, config):
        return {**state, "thread": config["configurable"]["thread_id"]}

    result = asyncio.run(node({"q": "hello"}, {"configurable": {"thread_id": "thread-async-pos"}}))

    assert result == {"q": "hello", "thread": "thread-async-pos"}
    assert list(runtimes) == ["thread-async-pos"]


def test_langgraph_node_preserves_business_keyword_matching_runtime_control():
    runtime = FakeRuntime()

    @durable_node(runtime, step_id="decorator-step")
    def node(*, step_id):
        return step_id

    assert node(step_id="business-value") == "business-value"


def test_langgraph_async_node_preserves_business_keyword_matching_runtime_control():
    runtime = FakeRuntime()

    @durable_node(runtime, step_id="decorator-step")
    async def node(*, step_id):
        return step_id

    assert asyncio.run(node(step_id="business-value")) == "business-value"


def test_langgraph_runtime_factory_preserves_integer_zero_thread_id():
    runtimes = {}

    def factory(run_id):
        runtimes.setdefault(run_id, FakeRuntime(run_id))
        return runtimes[run_id]

    @durable_node(factory, step_id="lookup")
    def node(state, config):
        return state

    assert node(
        {"q": "hello"},
        config={"run_id": "trace-a", "configurable": {"thread_id": 0}},
    ) == {"q": "hello"}
    assert list(runtimes) == ["0"]
