from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol

from app.execution.models import ExecutionOutcome, OrderIntent
from app.market_data.snapshots import MarketStateInput


class EquityExecutionBroker(Protocol):
    def is_live_capable(self) -> bool: ...

    def execute_order(
        self,
        intent: OrderIntent,
        *,
        source_metadata: dict[str, object],
        market_state: MarketStateInput | None,
    ) -> ExecutionOutcome: ...


@dataclass(frozen=True, slots=True)
class UnsupportedLiveEquityBroker:
    reason: str = "live_equity_broker_unconfigured"

    def is_live_capable(self) -> bool:
        return False

    def execute_order(
        self,
        intent: OrderIntent,
        *,
        source_metadata: dict[str, object],
        market_state: MarketStateInput | None,
    ) -> ExecutionOutcome:
        raise RuntimeError(self.reason)


@dataclass(frozen=True, slots=True)
class ManualLiveEquityBroker:
    venue_name: str = "live_equity"
    status: str = "submitted_manual"

    def is_live_capable(self) -> bool:
        return False

    def execute_order(
        self,
        intent: OrderIntent,
        *,
        source_metadata: dict[str, object],
        market_state: MarketStateInput | None,
    ) -> ExecutionOutcome:
        reference_price = None if market_state is None else market_state.reference_price
        price_cents = None
        if reference_price is not None:
            price_cents = int((reference_price * Decimal("100")).to_integral_value())
        return ExecutionOutcome(
            client_order_id=intent.order_id,
            instrument=intent.instrument,
            side=intent.side,
            action=intent.order_type,
            status=self.status,
            requested_quantity=int(intent.quantity),
            filled_quantity=0,
            price_cents=price_cents,
            time_in_force=intent.time_in_force,
            is_flatten=False,
            created_at=intent.created_at,
            metadata={
                **source_metadata,
                "broker_mode": "manual_live_equity",
                "reference_price_cents": price_cents,
            },
        )
