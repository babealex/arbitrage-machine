from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from app.core.time import is_stale


@dataclass(slots=True)
class TradingReadiness:
    can_trade: bool
    halt_reasons: list[str] = field(default_factory=list)
    details: dict[str, object] = field(default_factory=dict)


def assess_layer2_trading_readiness(
    *,
    market_data_last_update: datetime,
    max_market_data_staleness_seconds: float,
    external_stale: bool,
    external_errors: list[str],
    app_mode: str,
    reconciliation_success: bool,
    reconciliation_mismatches: list[str],
    now: datetime,
) -> TradingReadiness:
    halt_reasons: list[str] = []
    details = {
        "market_data_last_update": market_data_last_update.isoformat(),
        "market_data_stale": is_stale(market_data_last_update, max_market_data_staleness_seconds, now=now),
        "external_stale": external_stale,
        "external_errors": list(external_errors),
        "app_mode": app_mode,
        "reconciliation_success": reconciliation_success,
        "reconciliation_mismatches": list(reconciliation_mismatches),
    }
    if details["market_data_stale"]:
        halt_reasons.append("stale_market_data")
    if external_stale and app_mode in {"balanced", "aggressive"}:
        halt_reasons.append("external_data_invalid")
    if not reconciliation_success:
        halt_reasons.append("reconciliation_failed")
    if reconciliation_mismatches:
        halt_reasons.append("reconciliation_mismatch")
    return TradingReadiness(
        can_trade=not halt_reasons,
        halt_reasons=halt_reasons,
        details=details,
    )


def assess_portfolio_runtime_readiness(
    *,
    live_trading: bool,
    live_enabled_sleeves: list[str],
    risk_killed: bool,
    risk_reasons: list[str],
    kalshi_context: dict[str, object] | None,
    kalshi_live_capable: bool,
    trend_live_capable: bool,
    options_live_capable: bool,
) -> TradingReadiness:
    halt_reasons: list[str] = []
    degraded_capabilities: list[str] = []
    capability_map = {
        "kalshi": kalshi_live_capable,
        "trend": trend_live_capable,
        "options": options_live_capable,
    }
    unsupported_live_sleeves: list[str] = []
    details: dict[str, object] = {
        "live_trading": live_trading,
        "live_enabled_sleeves": list(live_enabled_sleeves),
        "risk_killed": risk_killed,
        "risk_reasons": list(risk_reasons),
        "kalshi_context": dict(kalshi_context or {}),
        "kalshi_live_capable": kalshi_live_capable,
        "trend_live_capable": trend_live_capable,
        "options_live_capable": options_live_capable,
        "live_capable_sleeves": [name for name, supported in capability_map.items() if supported],
    }
    if risk_killed:
        halt_reasons.append("portfolio_risk_killed")
    if live_trading:
        if not kalshi_live_capable:
            degraded_capabilities.append("kalshi_auth_missing")
        if not trend_live_capable:
            degraded_capabilities.append("trend_paper_only")
        if not options_live_capable:
            degraded_capabilities.append("options_paper_only")
        for sleeve in live_enabled_sleeves:
            if not capability_map.get(sleeve, False):
                unsupported_live_sleeves.append(sleeve)
                halt_reasons.append(f"unsupported_live_sleeve:{sleeve}")
        if kalshi_context and kalshi_context.get("trading_readiness") is False:
            for reason in kalshi_context.get("trading_halt_reasons", []):
                if reason not in halt_reasons:
                    halt_reasons.append(str(reason))
    details["degraded_capabilities"] = degraded_capabilities
    details["unsupported_live_sleeves"] = unsupported_live_sleeves
    return TradingReadiness(
        can_trade=not halt_reasons,
        halt_reasons=halt_reasons,
        details=details,
    )
