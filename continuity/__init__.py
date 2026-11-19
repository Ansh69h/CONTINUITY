"""CONTINUITY v0.8.0 — Causal Recovery, Distributed Storage & Reliability Validation."""

from .models import (
    CausalEvent,
    EventType,
    SideEffectClass,
    RecoveryDecision,
    RecoveryAction,
    ExecutionRecord,
    InvariantViolation,
    safe_json_default,
)
from .storage.base import CausalStoreProtocol, InMemoryCausalStore, SQLiteCausalStore
from .storage.backends import PostgresCausalStore, RedisCausalStore
from .telemetry import export_otlp_traces, build_otlp_payload
from .planner import RecoveryPlanner, AsyncRecoveryPlanner
from .runtime import (
    ContinuityRuntime,
    recoverable,
    recoverable_async,
    HttpBridge,
    AsyncHttpBridge,
)
from .validation.invariants import (
    SEVEN_INVARIANTS,
    InvariantChecker,
    InvariantReport,
)
from .validation.harness import (
    FaultInjector,
    StatefulModelChecker,
    TrialResult,
    ValidationCampaignReport,
    run_reliability_campaign,
)

__version__ = "0.8.0"

__all__ = [
    "__version__",
    "CausalEvent",
    "EventType",
    "SideEffectClass",
    "RecoveryDecision",
    "RecoveryAction",
    "ExecutionRecord",
    "InvariantViolation",
    "safe_json_default",
    "CausalStoreProtocol",
    "InMemoryCausalStore",
    "SQLiteCausalStore",
    "PostgresCausalStore",
    "RedisCausalStore",
    "export_otlp_traces",
    "build_otlp_payload",
    "RecoveryPlanner",
    "AsyncRecoveryPlanner",
    "ContinuityRuntime",
    "recoverable",
    "recoverable_async",
    "HttpBridge",
    "AsyncHttpBridge",
    "SEVEN_INVARIANTS",
    "InvariantChecker",
    "InvariantReport",
    "FaultInjector",
    "StatefulModelChecker",
    "TrialResult",
    "ValidationCampaignReport",
    "run_reliability_campaign",
]
