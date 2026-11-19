from __future__ import annotations

import functools
import inspect
from collections.abc import Mapping
from typing import Any, Callable, Optional, Tuple, TypeVar, Union

from continuity import SideEffectClass
from .protocol import DurableRuntime

F = TypeVar("F", bound=Callable[..., Any])
RuntimeFactory = Callable[[str], DurableRuntime]
RuntimeProvider = Union[DurableRuntime, RuntimeFactory]
_MISSING = object()


def _runtime_for(provider: RuntimeProvider, config: Any) -> DurableRuntime:
    if callable(getattr(provider, "step", None)):
        return provider  # type: ignore[return-value]
    if not isinstance(config, Mapping):
        raise ValueError("A runtime factory requires LangGraph config with a stable thread_id")
    configurable = config.get("configurable", {})
    thread_id = configurable.get("thread_id")
    if isinstance(thread_id, (str, int)) and not isinstance(thread_id, bool) and str(thread_id):
        run_id = thread_id
    else:
        run_id = config.get("run_id")
    if not isinstance(run_id, (str, int)) or isinstance(run_id, bool) or not str(run_id):
        raise ValueError("A runtime factory requires config['configurable']['thread_id'] or config['run_id']")
    return provider(str(run_id))  # type: ignore[operator]


def _split_config(signature: inspect.Signature, args: tuple[Any, ...], kwargs: dict[str, Any]):
    parameter = signature.parameters.get("config")
    if parameter is None:
        return _MISSING, args, dict(kwargs), None

    bound = signature.bind_partial(*args, **kwargs)
    config = bound.arguments.get("config", _MISSING)
    if config is _MISSING:
        return config, args, dict(kwargs), None

    business_args = list(args)
    business_kwargs = dict(kwargs)
    if "config" in business_kwargs:
        business_kwargs.pop("config")
        config_position = None
    elif parameter.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD):
        positional_names = [
            name
            for name, value in signature.parameters.items()
            if value.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
        ]
        config_position = positional_names.index("config")
        business_args.pop(config_position)
    else:
        config_position = None
    return config, tuple(business_args), business_kwargs, config_position


def _invoke_with_config(node: Callable[..., Any], config: Any, config_position: int | None, args, kwargs):
    node_args = list(args)
    node_kwargs = dict(kwargs)
    if config is not _MISSING:
        if config_position is None:
            node_kwargs["config"] = config
        else:
            node_args.insert(config_position, config)
    return node(*node_args, **node_kwargs)


def durable_node(
    runtime: RuntimeProvider,
    *,
    step_id: str,
    side_effect: SideEffectClass = SideEffectClass.PURE,
    reconciler: Optional[Callable[[str], Tuple[bool, Any]]] = None,
    compensator: Optional[Callable[[str], Any]] = None,
) -> Callable[[F], F]:
    """Decorate one LangGraph node function with CONTINUITY step durability.

    LangGraph's ``config`` parameter is forwarded to the node but excluded from
    the idempotency input. Use a factory to select a runtime by ``thread_id``.
    """
    if not step_id:
        raise ValueError("step_id must be a non-empty stable identifier")

    def decorate(node: F) -> F:
        signature = inspect.signature(node)
        accepts_config = "config" in signature.parameters
        is_async = inspect.iscoroutinefunction(node)

        if is_async:
            @functools.wraps(node)
            async def wrapped(*args: Any, **kwargs: Any) -> Any:
                if accepts_config:
                    config, args, kwargs, config_position = _split_config(signature, args, kwargs)
                else:
                    config, config_position = _MISSING, None
                selected_runtime = _runtime_for(runtime, None if config is _MISSING else config)

                async def invoke(input_envelope: Any) -> Any:
                    node_args, node_kwargs = input_envelope
                    return await _invoke_with_config(node, config, config_position, node_args, node_kwargs)

                return await selected_runtime.step_async(
                    invoke,
                    (args, kwargs),
                    step_id=step_id,
                    step_name=getattr(node, "__name__", step_id),
                    side_effect=side_effect,
                    reconciler=reconciler,
                    compensator=compensator,
                )

            return wrapped  # type: ignore[return-value]

        @functools.wraps(node)
        def wrapped(*args: Any, **kwargs: Any) -> Any:
            if accepts_config:
                config, args, kwargs, config_position = _split_config(signature, args, kwargs)
            else:
                config, config_position = _MISSING, None
            selected_runtime = _runtime_for(runtime, None if config is _MISSING else config)

            def invoke(input_envelope: Any) -> Any:
                node_args, node_kwargs = input_envelope
                return _invoke_with_config(node, config, config_position, node_args, node_kwargs)

            return selected_runtime.step(
                invoke,
                (args, kwargs),
                step_id=step_id,
                step_name=getattr(node, "__name__", step_id),
                side_effect=side_effect,
                reconciler=reconciler,
                compensator=compensator,
            )

        return wrapped  # type: ignore[return-value]

    return decorate
