from __future__ import annotations

from dataclasses import asdict, dataclass

from app.runtime.health import TradingReadiness


@dataclass(frozen=True, slots=True)
class PortfolioDeploymentReadiness:
    orchestrator_enabled: bool
    live_trading: bool
    live_enabled_sleeves: list[str]
    live_capable_sleeves: list[str]
    degraded_sleeves: list[str]
    unsupported_live_sleeves: list[str]
    kalshi_auth_configured: bool
    kill_switch_active: bool
    kill_switch_reasons: list[str]
    latest_cycle_run_id: str | None
    latest_replayable_run_id: str | None
    runtime_can_trade: bool
    halt_reasons: list[str]

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def build_portfolio_deployment_readiness(
    *,
    orchestrator_enabled: bool,
    live_trading: bool,
    live_enabled_sleeves: list[str],
    kalshi_auth_configured: bool,
    portfolio_readiness: TradingReadiness,
    latest_cycle_run_id: str | None,
    latest_replayable_run_id: str | None,
) -> PortfolioDeploymentReadiness:
    details = portfolio_readiness.details
    return PortfolioDeploymentReadiness(
        orchestrator_enabled=orchestrator_enabled,
        live_trading=live_trading,
        live_enabled_sleeves=list(live_enabled_sleeves),
        live_capable_sleeves=list(details.get("live_capable_sleeves", [])),
        degraded_sleeves=list(details.get("degraded_capabilities", [])),
        unsupported_live_sleeves=list(details.get("unsupported_live_sleeves", [])),
        kalshi_auth_configured=kalshi_auth_configured,
        kill_switch_active=bool(details.get("risk_killed", False)),
        kill_switch_reasons=list(details.get("risk_reasons", [])),
        latest_cycle_run_id=latest_cycle_run_id,
        latest_replayable_run_id=latest_replayable_run_id,
        runtime_can_trade=portfolio_readiness.can_trade,
        halt_reasons=list(portfolio_readiness.halt_reasons),
    )
