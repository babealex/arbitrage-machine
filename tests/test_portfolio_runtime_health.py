from __future__ import annotations

from app.portfolio.deployment import build_portfolio_deployment_readiness
from app.runtime.health import assess_portfolio_runtime_readiness


def test_portfolio_runtime_readiness_marks_paper_only_sleeves_as_degraded_in_live_mode() -> None:
    readiness = assess_portfolio_runtime_readiness(
        live_trading=True,
        live_enabled_sleeves=["kalshi"],
        risk_killed=False,
        risk_reasons=[],
        kalshi_context={"trading_readiness": True, "trading_halt_reasons": []},
        kalshi_live_capable=True,
        trend_live_capable=False,
        options_live_capable=False,
    )

    assert readiness.can_trade is True
    assert "trend_paper_only" in readiness.details["degraded_capabilities"]
    assert "options_paper_only" in readiness.details["degraded_capabilities"]


def test_portfolio_runtime_readiness_fails_closed_when_risk_killed() -> None:
    readiness = assess_portfolio_runtime_readiness(
        live_trading=True,
        live_enabled_sleeves=["kalshi"],
        risk_killed=True,
        risk_reasons=["daily_loss_limit"],
        kalshi_context={"trading_readiness": True, "trading_halt_reasons": []},
        kalshi_live_capable=True,
        trend_live_capable=False,
        options_live_capable=False,
    )

    assert readiness.can_trade is False
    assert "portfolio_risk_killed" in readiness.halt_reasons


def test_portfolio_runtime_readiness_fails_closed_for_unsupported_live_sleeve() -> None:
    readiness = assess_portfolio_runtime_readiness(
        live_trading=True,
        live_enabled_sleeves=["kalshi", "trend", "options"],
        risk_killed=False,
        risk_reasons=[],
        kalshi_context={"trading_readiness": True, "trading_halt_reasons": []},
        kalshi_live_capable=True,
        trend_live_capable=False,
        options_live_capable=False,
    )

    assert readiness.can_trade is False
    assert "unsupported_live_sleeve:trend" in readiness.halt_reasons
    assert "unsupported_live_sleeve:options" in readiness.halt_reasons
    assert readiness.details["unsupported_live_sleeves"] == ["trend", "options"]


def test_deployment_readiness_exposes_latest_replayable_run_and_halts() -> None:
    readiness = assess_portfolio_runtime_readiness(
        live_trading=True,
        live_enabled_sleeves=["kalshi", "trend"],
        risk_killed=False,
        risk_reasons=[],
        kalshi_context={"trading_readiness": False, "trading_halt_reasons": ["stale_market_data"]},
        kalshi_live_capable=True,
        trend_live_capable=False,
        options_live_capable=False,
    )

    deployment = build_portfolio_deployment_readiness(
        orchestrator_enabled=True,
        live_trading=True,
        live_enabled_sleeves=["kalshi", "trend"],
        kalshi_auth_configured=True,
        portfolio_readiness=readiness,
        latest_cycle_run_id="run-1",
        latest_replayable_run_id="run-1",
    )

    assert deployment.runtime_can_trade is False
    assert deployment.latest_replayable_run_id == "run-1"
    assert deployment.unsupported_live_sleeves == ["trend"]
    assert "stale_market_data" in deployment.halt_reasons
