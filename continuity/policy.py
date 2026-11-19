from __future__ import annotations

from dataclasses import dataclass
from .models import SideEffectClass


@dataclass(frozen=True)
class SideEffectPolicy:
    """Optional recovery requirements layered over an existing side-effect class."""

    side_effect: SideEffectClass
    require_reconciler: bool = False
    require_compensator: bool = False
    allow_operator_approval: bool = False
    retry_uncertain_without_reconciliation: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.side_effect, SideEffectClass):
            raise TypeError("side_effect must be a SideEffectClass")
        if self.require_compensator and self.side_effect != SideEffectClass.COMPENSATABLE:
            raise ValueError("require_compensator is only valid for COMPENSATABLE steps")
        if self.retry_uncertain_without_reconciliation:
            raise ValueError("Uncertain effects cannot be retried without reconciliation or operator approval")
