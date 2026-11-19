"""CONTINUITY v0.8.0 — Causal Recovery, Distributed Storage & Reliability Validation."""

from .models import (
    CausalEvent,
    CURRENT_EVENT_SCHEMA_VERSION,
    EventType,
    SideEffectClass,
    RecoveryDecision,
    RecoveryAction,
    ExecutionRecord,
    InvariantViolation,
    safe_json_default,
)
from .policy import SideEffectPolicy
from .storage.base import (
    CausalStoreProtocol,
    FencingTokenError,
    InMemoryCausalStore,
    SQLiteCausalStore,
    StepLease,
)
from .storage.backends import PostgresCausalStore, RedisCausalStore
from .telemetry import build_otlp_payload, build_runtime_metrics, export_otlp_traces, log_causal_event
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
    "CURRENT_EVENT_SCHEMA_VERSION",
    "EventType",
    "SideEffectClass",
    "SideEffectPolicy",
    "RecoveryDecision",
    "RecoveryAction",
    "ExecutionRecord",
    "InvariantViolation",
    "safe_json_default",
    "CausalStoreProtocol",
    "StepLease",
    "FencingTokenError",
    "InMemoryCausalStore",
    "SQLiteCausalStore",
    "PostgresCausalStore",
    "RedisCausalStore",
    "export_otlp_traces",
    "build_otlp_payload",
    "build_runtime_metrics",
    "log_causal_event",
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
