from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from app.config import Settings
from app.db import Database
from app.persistence.convexity_repo import ConvexityRepository
from app.strategies.convexity.lifecycle import export_allocator_candidates, run_convexity_cycle
from app.strategies.convexity.models import ConvexityAccountState, OptionContractQuote


def test_convexity_cycle_persists_summary_and_orders(tmp_path: Path) -> None:
    db = Database(tmp_path / "convexity_lifecycle.db")
    settings = Settings()
    settings.convexity_persistence_enabled = True
    settings.convexity_paper_execution_enabled = True
    now = datetime(2026, 3, 22, 12, 0, tzinfo=timezone.utc)
    option_chain = _liquid_chain(now.date())
    market_state = {
        "SPY": {
            "spot_price": Decimal("500"),
            "option_chain": option_chain,
            "realized_vol_pct": Decimal("0.03"),
            "implied_vol_pct": Decimal("0.02"),
            "breakout_score": Decimal("0.04"),
            "volume_spike_ratio": Decimal("2.0"),
            "as_of": now,
        }
    }
    calendars = {
        "SPY": [
            {
                "setup_type": "macro",
                "event_time": now + timedelta(days=4),
                "event_history_median_move": Decimal("0.04"),
                "direction": "bullish",
            }
        ]
    }
    cycle = run_convexity_cycle(
        db,
        settings,
        market_state,
        calendars,
        {"SPY": {"direction": "bullish"}},
        {"SPY": option_chain},
        ConvexityAccountState(nav=Decimal("10000"), available_cash=Decimal("10000")),
    )

    repo = ConvexityRepository(db)
    summary = repo.load_latest_run_summary()
    assert summary is not None
    assert cycle.summary.accepted_trades >= 1
    assert summary["accepted_trades"] >= 1
    assert len(cycle.order_intents) >= 1
    exported = export_allocator_candidates(cycle)
    assert len(exported) == len(cycle.selected_candidates)
    assert all(item.candidate.strategy_family == "convexity" for item in exported)


def test_convexity_cycle_no_trade_still_persists_summary(tmp_path: Path) -> None:
    db = Database(tmp_path / "convexity_none.db")
    settings = Settings()
    now = datetime(2026, 3, 22, 12, 0, tzinfo=timezone.utc)
    cycle = run_convexity_cycle(
        db,
        settings,
        {
            "SPY": {
                "spot_price": Decimal("500"),
                "option_chain": [],
                "realized_vol_pct": Decimal("0.01"),
                "implied_vol_pct": Decimal("0.04"),
                "breakout_score": Decimal("0.01"),
                "volume_spike_ratio": Decimal("1.0"),
                "as_of": now,
            }
        },
        {"SPY": []},
        {},
        {"SPY": []},
        ConvexityAccountState(nav=Decimal("1000"), available_cash=Decimal("1000")),
    )
    repo = ConvexityRepository(db)
    summary = repo.load_latest_run_summary()
    assert summary is not None
    assert cycle.summary.scanned_opportunities == 0
    assert summary["candidate_trades"] == 0
    assert summary["accepted_trades"] == 0
    assert cycle.order_intents == []


def _liquid_chain(as_of: date) -> list[OptionContractQuote]:
    expiry = as_of + timedelta(days=11)
    return [
        OptionContractQuote("SPY_510C", expiry, Decimal("510"), "call", Decimal("1.00"), Decimal("1.20"), Decimal("1.10"), Decimal("0.20"), Decimal("0.25"), None, None, None, 500, 200),
        OptionContractQuote("SPY_520C", expiry, Decimal("520"), "call", Decimal("0.45"), Decimal("0.55"), Decimal("0.50"), Decimal("0.18"), Decimal("0.18"), None, None, None, 400, 150),
        OptionContractQuote("SPY_510P", expiry, Decimal("510"), "put", Decimal("0.95"), Decimal("1.10"), Decimal("1.025"), Decimal("0.20"), Decimal("-0.25"), None, None, None, 500, 200),
        OptionContractQuote("SPY_500C", expiry, Decimal("500"), "call", Decimal("4.80"), Decimal("5.00"), Decimal("4.90"), Decimal("0.19"), Decimal("0.50"), None, None, None, 600, 300),
        OptionContractQuote("SPY_500P", expiry, Decimal("500"), "put", Decimal("4.70"), Decimal("4.90"), Decimal("4.80"), Decimal("0.19"), Decimal("-0.50"), None, None, None, 600, 300),
    ]
