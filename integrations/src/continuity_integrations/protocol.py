from __future__ import annotations

import functools
import inspect
from typing import Any, Callable, Optional, Protocol, Tuple, TypeVar

from continuity import SideEffectClass

F = TypeVar("F", bound=Callable[..., Any])


class DurableRuntime(Protocol):
    """The public CONTINUITY runtime surface used by adapters."""

    def step(
        self,
        fn: Callable[..., Any],
        *args: Any,
        step_id: Optional[str] = None,
        step_name: Optional[str] = None,
        side_effect: SideEffectClass = SideEffectClass.PURE,
        reconciler: Optional[Callable[[str], Tuple[bool, Any]]] = None,
        compensator: Optional[Callable[[str], Any]] = None,
        **kwargs: Any,
    ) -> Any: ...

    async def step_async(
        self,
        fn: Callable[..., Any],
        *args: Any,
        step_id: Optional[str] = None,
        step_name: Optional[str] = None,
        side_effect: SideEffectClass = SideEffectClass.PURE,
        reconciler: Optional[Callable[[str], Any]] = None,
        compensator: Optional[Callable[[str], Any]] = None,
        **kwargs: Any,
    ) -> Any: ...


def durable_callable(
    runtime: DurableRuntime,
    *,
    step_id: str,
    side_effect: SideEffectClass = SideEffectClass.PURE,
    reconciler: Optional[Callable[[str], Any]] = None,
    compensator: Optional[Callable[[str], Any]] = None,
) -> Callable[[F], F]:
    """Wrap a sync or async callable in a public CONTINUITY runtime step."""
    if not step_id:
        raise ValueError("step_id must be a non-empty stable identifier")

    def decorate(fn: F) -> F:
        if inspect.iscoroutinefunction(fn):
            @functools.wraps(fn)
            async def async_wrapped(*args: Any, **kwargs: Any) -> Any:
                async def invoke(call_args: tuple[Any, ...], call_kwargs: dict[str, Any]) -> Any:
                    return await fn(*call_args, **call_kwargs)

                return await runtime.step_async(
                    invoke,
                    args,
                    kwargs,
                    step_id=step_id,
                    step_name=getattr(fn, "__name__", step_id),
                    side_effect=side_effect,
                    reconciler=reconciler,
                    compensator=compensator,
                )

            return async_wrapped  # type: ignore[return-value]

        @functools.wraps(fn)
        def sync_wrapped(*args: Any, **kwargs: Any) -> Any:
            def invoke(call_args: tuple[Any, ...], call_kwargs: dict[str, Any]) -> Any:
                return fn(*call_args, **call_kwargs)

            return runtime.step(
                invoke,
                args,
                kwargs,
                step_id=step_id,
                step_name=getattr(fn, "__name__", step_id),
                side_effect=side_effect,
                reconciler=reconciler,
                compensator=compensator,
            )

        return sync_wrapped  # type: ignore[return-value]

    return decorate
