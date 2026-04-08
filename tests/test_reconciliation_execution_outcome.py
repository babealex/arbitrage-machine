from __future__ import annotations

from pathlib import Path

from app.db import Database
from app.persistence.kalshi_repo import KalshiRepository
from app.portfolio.state import PortfolioState


class ReconcilingClient:
    data_only_mode = False

    def get_balance(self):
        return {"equity": 1000, "cash": 950, "pnl_today": 5}

    def get_positions(self):
        return [{"ticker": "FED-1", "yes_count": 1, "no_count": 0, "market_price": 0.52}]

    def list_orders(self):
        return []

    def get_fills(self):
        return [
            {
                "order_id": "oid-1",
                "ticker": "FED-1",
                "count": 3,
                "price": 41,
                "side": "yes",
                "time_in_force": "ioc",
            }
        ]


def test_reconciliation_persists_typed_execution_outcome(tmp_path: Path) -> None:
    repo = KalshiRepository(Database(tmp_path / "reconcile.db"))
    portfolio = PortfolioState(ReconcilingClient())

    result = portfolio.reconcile(repo)

    outcomes = repo.load_execution_outcomes()
    assert result.success
    assert len(outcomes) == 1
    assert outcomes[0].status == "reconciled_fill"
    assert outcomes[0].filled_quantity == 3
    assert outcomes[0].price_cents == 41
    assert outcomes[0].metadata["raw_fill"]["ticker"] == "FED-1"
