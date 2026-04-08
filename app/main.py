from __future__ import annotations

from app.common.bootstrap import load_runtime_settings
from app.events.runtime import run_event_driven
from app.layer3_belief.runtime import run_layer3_belief
from app.portfolio.runtime import run_portfolio_orchestrator
from app.runtime.two_sleeve import run_two_sleeve_paper_runtime


def run() -> None:
    settings = load_runtime_settings()
    if settings.enable_event_driven_mode and settings.event_trading_only_mode:
        run_event_driven(settings)
        return
    if settings.enable_belief_layer and settings.belief_layer_only_mode:
        run_layer3_belief(settings)
        return
    if settings.enable_portfolio_orchestrator:
        run_portfolio_orchestrator(settings)
        return
    if settings.enable_two_sleeve_runtime:
        run_two_sleeve_paper_runtime(settings)
        return
    from app.layer2_kalshi.runtime import run_layer2_kalshi

    run_layer2_kalshi(settings)


if __name__ == "__main__":
    run()
