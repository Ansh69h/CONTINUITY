from __future__ import annotations

from typing import Any, Callable, Optional, Tuple, TypeVar

from continuity import SideEffectClass
from .protocol import DurableRuntime, durable_callable

F = TypeVar("F", bound=Callable[..., Any])


def durable_function_tool(
    runtime: DurableRuntime,
    *,
    step_id: str,
    side_effect: SideEffectClass = SideEffectClass.PURE,
    reconciler: Optional[Callable[[str], Tuple[bool, Any]]] = None,
    compensator: Optional[Callable[[str], Any]] = None,
    **tool_options: Any,
) -> Callable[[F], Any]:
    """Create an OpenAI Agents SDK function tool with a durable local handler.

    This protects function tools executed in the application process. It does
    not wrap a whole Agent/Runner execution or hosted tools.
    """
    try:
        from agents.decorators import tool
    except ImportError as exc:
        raise ImportError(
            "Install the optional adapter dependency with `pip install '.[openai-agents]'`"
        ) from exc

    def decorate(fn: F) -> Any:
        durable_fn = durable_callable(
            runtime,
            step_id=step_id,
            side_effect=side_effect,
            reconciler=reconciler,
            compensator=compensator,
        )(fn)
        if tool_options:
            return tool(**tool_options)(durable_fn)
        return tool(durable_fn)

    return decorate
