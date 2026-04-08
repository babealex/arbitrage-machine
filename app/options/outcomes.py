from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass(frozen=True, slots=True)
class OptionOutcomeEvaluation:
    underlying: str
    decision_observed_at: datetime
    evaluation_horizon: str
    evaluated_at: datetime
    selected_structure_name: str
    expected_pnl: Decimal
    realized_return: Decimal
    realized_vol: Decimal | None
    realized_iv_change: Decimal | None
    realized_pnl: Decimal | None
    max_drawdown_realized: Decimal | None
    predicted_pnl_error: Decimal | None
    move_error: Decimal | None
    vol_error: Decimal | None
    iv_error: Decimal | None
