from __future__ import annotations
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any, Dict, List, Optional
import time


class EventType(str, Enum):
    STEP_INTENT = "STEP_INTENT"
    STEP_STARTED = "STEP_STARTED"
    STEP_COMMITTED = "STEP_COMMITTED"
    STEP_FAILED = "STEP_FAILED"
    COMPENSATION_INTENT = "COMPENSATION_INTENT"
    COMPENSATION_COMMITTED = "COMPENSATION_COMMITTED"
    RECOVERY_DECISION = "RECOVERY_DECISION"
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

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["event_type"] = self.event_type.value
        d["side_effect"] = self.side_effect.value
        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CausalEvent":
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
