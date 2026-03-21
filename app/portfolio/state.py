from __future__ import annotations

from dataclasses import dataclass, field

from app.kalshi.rest_client import KalshiRestClient
from app.models import Fill, PortfolioSnapshot, Position, Side
from app.persistence.kalshi_repo import KalshiRepository


@dataclass(slots=True)
class ReconciliationResult:
    snapshot: PortfolioSnapshot
    open_orders: list[dict] = field(default_factory=list)
    fills: list[dict] = field(default_factory=list)
    mismatches: list[str] = field(default_factory=list)
    success: bool = True


class PortfolioState:
    def __init__(self, client: KalshiRestClient) -> None:
        self.client = client
        self.snapshot = PortfolioSnapshot(equity=1000.0, cash=1000.0, daily_pnl=0.0, positions={})

    def reconcile(self, repo: KalshiRepository | None = None) -> ReconciliationResult:
        if self.client.data_only_mode:
            if repo is not None:
                repo.replace_positions(self.snapshot)
            return ReconciliationResult(snapshot=self.snapshot, open_orders=[], fills=[], mismatches=[], success=True)
        try:
            balance = self.client.get_balance()
            positions = self.client.get_positions()
            open_orders = self.client.list_orders()
            fills = self.client.get_fills()
        except Exception:
            return ReconciliationResult(snapshot=self.snapshot, success=False, mismatches=["exchange_unreachable"])
        snapshot_positions: dict[str, Position] = {}
        for raw in positions:
            ticker = raw.get("ticker", "")
            snapshot_positions[ticker] = Position(
                ticker=ticker,
                yes_contracts=int(raw.get("yes_count", 0)),
                no_contracts=int(raw.get("no_count", 0)),
                mark_price=float(raw.get("market_price", 0.0)),
            )
        self.snapshot = PortfolioSnapshot(
            equity=float(balance.get("equity", self.snapshot.equity)),
            cash=float(balance.get("cash", self.snapshot.cash)),
            daily_pnl=float(balance.get("pnl_today", self.snapshot.daily_pnl)),
            positions=snapshot_positions,
        )
        mismatches: list[str] = []
        if repo is not None:
            repo.replace_positions(self.snapshot)
            for raw_fill in fills:
                repo.persist_fill(
                    Fill(
                        order_id=str(raw_fill.get("order_id", raw_fill.get("orderId", ""))),
                        ticker=str(raw_fill.get("ticker", "")),
                        quantity=int(raw_fill.get("count", raw_fill.get("quantity", 0))),
                        price=int(raw_fill.get("price", 0)),
                        side=Side(str(raw_fill.get("side", "yes")).lower()),
                    )
                )
            local_open = repo.open_order_ids()
            exchange_open = {str(order.get("client_order_id", order.get("clientOrderId", ""))) for order in open_orders}
            missing = sorted(order_id for order_id in local_open if order_id and order_id not in exchange_open)
            if missing:
                mismatches.append(f"missing_open_orders:{','.join(missing)}")
        return ReconciliationResult(snapshot=self.snapshot, open_orders=open_orders, fills=fills, mismatches=mismatches, success=True)
