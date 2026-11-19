from __future__ import annotations
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any, Dict, List, Optional
import time


CURRENT_EVENT_SCHEMA_VERSION = 1


class EventType(str, Enum):
    STEP_INTENT = "STEP_INTENT"
    STEP_STARTED = "STEP_STARTED"
    STEP_COMMITTED = "STEP_COMMITTED"
    STEP_FAILED = "STEP_FAILED"
    COMPENSATION_INTENT = "COMPENSATION_INTENT"
    COMPENSATION_COMMITTED = "COMPENSATION_COMMITTED"
    RECOVERY_DECISION = "RECOVERY_DECISION"
    RECOVERY_APPROVED = "RECOVERY_APPROVED"
    SNAPSHOT_SAVED = "SNAPSHOT_SAVED"


class SideEffectClass(str, Enum):
    PURE = "PURE"
    IDEMPOTENT = "IDEMPOTENT"
    COMPENSATABLE = "COMPENSATABLE"
    IRREVERSIBLE = "IRREVERSIBLE"


class RecoveryAction(str, Enum):
    REUSE_CACHED = "REUSE_CACHED"
    REEXECUTE = "REEXECUTE"
    RECONCILE_EXTERNAL = "RECONCILE_EXTERNAL"
    COMPENSATE_AND_RETRY = "COMPENSATE_AND_RETRY"
    HALT_ESCALATE = "HALT_ESCALATE"


def safe_json_default(obj: Any) -> Any:
    """Deterministic, fault-tolerant JSON serialization fallback for AI models, dataclasses, and primitives."""
    if hasattr(obj, "model_dump") and callable(obj.model_dump):
        return obj.model_dump()
    if hasattr(obj, "dict") and callable(obj.dict):
        return obj.dict()
    if hasattr(obj, "__dataclass_fields__"):
        return asdict(obj)
    if hasattr(obj, "isoformat") and callable(obj.isoformat):
        return obj.isoformat()
    if isinstance(obj, (set, frozenset)):
        return sorted(list(obj), key=str)
    if isinstance(obj, bytes):
        return obj.decode("utf-8", errors="replace")
    if isinstance(obj, Enum):
        return obj.value
    if hasattr(obj, "__dict__"):
        return {k: v for k, v in sorted(obj.__dict__.items()) if not k.startswith("_")}
    return str(obj)


@dataclass(frozen=True)
class CausalEvent:
    run_id: str
    seq: int
    event_id: str
    step_id: str
    step_name: str
    event_type: EventType
    side_effect: SideEffectClass
    idempotency_key: str
    parent_event_ids: List[str] = field(default_factory=list)
    payload: Dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)
    worker_id: str = "default-worker"
    attempt: int = 1
    schema_version: int = CURRENT_EVENT_SCHEMA_VERSION

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["event_type"] = self.event_type.value
        d["side_effect"] = self.side_effect.value
        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CausalEvent":
        schema_version = data.get("schema_version", CURRENT_EVENT_SCHEMA_VERSION)
        if type(schema_version) is not int:
            raise ValueError("Event schema_version must be an integer")
        if schema_version != CURRENT_EVENT_SCHEMA_VERSION:
            raise ValueError(
                f"Unsupported event schema version {schema_version}; "
                f"this runtime supports version {CURRENT_EVENT_SCHEMA_VERSION}"
            )
        return cls(
            run_id=data["run_id"],
            seq=int(data["seq"]),
            event_id=data["event_id"],
            step_id=data["step_id"],
            step_name=data["step_name"],
            event_type=EventType(data["event_type"]),
            side_effect=SideEffectClass(data["side_effect"]),
            idempotency_key=data["idempotency_key"],
            parent_event_ids=list(data.get("parent_event_ids", [])),
            payload=dict(data.get("payload", {})),
            timestamp=float(data.get("timestamp", 0.0)),
            worker_id=data.get("worker_id", "default-worker"),
            attempt=int(data.get("attempt", 1)),
            schema_version=schema_version,
        )


@dataclass
class RecoveryDecision:
    step_id: str
    step_name: str
    action: RecoveryAction
    reason: str
    cached_output: Any = None
    attempt: int = 1


@dataclass
class ExecutionRecord:
    step_id: str
    step_name: str
    side_effect: SideEffectClass
    idempotency_key: str
    status: str
    attempts: int = 0
    output: Any = None
    error: Optional[str] = None
    compensated: bool = False


@dataclass
class InvariantViolation:
    invariant_id: str
    invariant_name: str
    run_id: str
    step_id: Optional[str]
    seq: Optional[int]
    description: str
    evidence: Dict[str, Any] = field(default_factory=dict)
