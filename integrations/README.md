# Optional framework integrations

This directory contains an independently installable adapter distribution. The CONTINUITY core remains dependency-free.

```powershell
python -m pip install -e integrations
python -m pip install -e "integrations[langgraph]"  # or [openai-agents], [fastapi], [all]
```

The package uses public CONTINUITY runtime methods and public framework entry points. It does not replace LangGraph's checkpointer, OpenAI Agents SDK sessions or tracing, or FastAPI's own request lifecycle. CONTINUITY records only wrapped steps. External effects still need stable step IDs, idempotency at the target service, and reconciliation or compensation callbacks where a retry could repeat an effect.

## Generic callable protocol

`continuity_integrations.protocol.durable_callable(runtime, step_id=..., side_effect=...)` wraps synchronous and asynchronous functions using `ContinuityRuntime.step()` and `step_async()`. It intentionally leaves scheduling, persistence, authentication, and external-service semantics to the host framework and application.

## LangGraph

Install with `pip install -e ".[langgraph]"`. `durable_node(runtime_or_factory, step_id=..., side_effect=...)` decorates a LangGraph node function. A callable runtime factory receives the LangGraph `configurable.thread_id` and should return a runtime with a durable store and that stable run ID. The adapter forwards LangGraph's `config` to the node but omits it from the step's idempotency inputs.

The wrapper does not create a graph checkpointer or replace LangGraph checkpointing. Keep a LangGraph checkpointer for graph state and resume; use CONTINUITY around nodes whose side effects or outputs need a separate durable execution record. The node wrapper handles one node call at a time and does not promise atomicity between a graph checkpoint and a CONTINUITY event.

The declared version range starts at LangGraph 1.2.23. The source-level wrapper itself has no LangGraph import. The local integration test ran against LangGraph 1.2.23, but no multi-version compatibility matrix has run.

Official API references: [LangGraph graph API](https://docs.langchain.com/oss/python/langgraph/graph-api), [LangGraph checkpointers](https://docs.langchain.com/oss/python/langgraph/persistence).

## OpenAI Agents SDK

Install with `pip install -e ".[openai-agents]"`. Use `durable_function_tool(...)` to wrap a local Python function as an Agents SDK `FunctionTool`. The adapter calls the SDK's public `agents.decorators.tool` decorator after wrapping the handler.

This integration covers local function tools only. It does not wrap `Runner.run`, hosted tools, agent handoffs, or Agents SDK session state. Store agent-run correlation in the application's chosen CONTINUITY `run_id`; maintain the SDK's own conversation/session state separately.

The declared version range starts at OpenAI Agents SDK 0.25.0. The local adapter test ran against 0.25.0 and only constructs a `FunctionTool`; no model call or provider request is made. No multi-version compatibility matrix has run.

Official API references: [Agents SDK function tools](https://openai.github.io/openai-agents-python/tools/), [Agents SDK quickstart](https://openai.github.io/openai-agents-python/quickstart/).

## FastAPI

Install with `pip install -e ".[fastapi]"`. `create_status_router(store, authorize=..., resume_handler=...)` provides authorized run listing, event inspection, recovery inspection, and an optional resume route. Payloads are omitted by default.

The authorization callback receives the request plus `action`, `run_id`, and `step_id`, and must return `False` to deny access. The resume callback is application-owned and must call the registered workflow through a fenced `ContinuityRuntime`; the adapter never loads code or callback names from request data. FastAPI's in-process `BackgroundTasks` should not be treated as a durable job queue.

The declared version range starts at FastAPI 0.158.0 to retain Python 3.10 compatibility. Local tests ran against FastAPI 0.165.0, including route registration, authorization, and resume-handler dispatch. This router is an inspection/control surface, not a multi-tenant security boundary by itself; applications must enforce their own authentication and run-level authorization.

Official API references: [FastAPI APIRouter and larger applications](https://fastapi.tiangolo.com/tutorial/bigger-applications/), [FastAPI BackgroundTasks](https://fastapi.tiangolo.com/tutorial/background-tasks/).

## Compatibility and verification

`integrations/pyproject.toml` declares the optional dependency bounds and package metadata. The generic wrappers have framework-independent tests; the optional-framework tests were run locally against LangGraph 1.2.23, OpenAI Agents SDK 0.25.0, and FastAPI 0.165.0. They do not contact a model provider. Keep these adapters marked experimental until the versions are covered in CI and a supported compatibility matrix is established.
