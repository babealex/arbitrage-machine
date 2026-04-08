from datetime import datetime, timedelta, timezone

from app.runtime.health import assess_layer2_trading_readiness


def test_readiness_fails_closed_on_stale_market_data() -> None:
    now = datetime.now(timezone.utc)
    readiness = assess_layer2_trading_readiness(
        market_data_last_update=now - timedelta(seconds=30),
        max_market_data_staleness_seconds=20,
        external_stale=False,
        external_errors=[],
        app_mode="safe",
        reconciliation_success=True,
        reconciliation_mismatches=[],
        now=now,
    )
    assert not readiness.can_trade
    assert "stale_market_data" in readiness.halt_reasons


def test_readiness_requires_external_validity_in_aggressive_modes() -> None:
    now = datetime.now(timezone.utc)
    readiness = assess_layer2_trading_readiness(
        market_data_last_update=now,
        max_market_data_staleness_seconds=20,
        external_stale=True,
        external_errors=["parse_failed"],
        app_mode="aggressive",
        reconciliation_success=True,
        reconciliation_mismatches=[],
        now=now,
    )
    assert not readiness.can_trade
    assert "external_data_invalid" in readiness.halt_reasons


def test_readiness_allows_safe_mode_without_external_confirmation() -> None:
    now = datetime.now(timezone.utc)
    readiness = assess_layer2_trading_readiness(
        market_data_last_update=now,
        max_market_data_staleness_seconds=20,
        external_stale=True,
        external_errors=[],
        app_mode="safe",
        reconciliation_success=True,
        reconciliation_mismatches=[],
        now=now,
    )
    assert readiness.can_trade


def test_readiness_fails_closed_on_reconciliation_problems() -> None:
    now = datetime.now(timezone.utc)
    readiness = assess_layer2_trading_readiness(
        market_data_last_update=now,
        max_market_data_staleness_seconds=20,
        external_stale=False,
        external_errors=[],
        app_mode="balanced",
        reconciliation_success=False,
        reconciliation_mismatches=["qty_mismatch"],
        now=now,
    )
    assert not readiness.can_trade
    assert "reconciliation_failed" in readiness.halt_reasons
    assert "reconciliation_mismatch" in readiness.halt_reasons
