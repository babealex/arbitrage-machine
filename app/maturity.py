from __future__ import annotations

"""Central maturity registry for major repo surfaces.

This repo is a paper-first multi-sleeve prop-trading research and execution
platform. Maturity labels are intended to keep runtime defaults, repo health,
and operator expectations aligned.
"""

from dataclasses import dataclass


STABLE = "stable"
ACTIVE_CORE = "active_core"
EXPERIMENTAL = "experimental"
DEPRECATED = "deprecated"


@dataclass(frozen=True, slots=True)
class ModuleMaturity:
    label: str
    rationale: str


MODULE_MATURITY: dict[str, ModuleMaturity] = {
    "app/layer2_kalshi/runtime.py": ModuleMaturity(
        label=STABLE,
        rationale="Tracked runtime for specialized Kalshi structural trading.",
    ),
    "app/events/runtime.py": ModuleMaturity(
        label=STABLE,
        rationale="Tracked event-driven paper execution path with hardened paper assumptions.",
    ),
    "app/layer3_belief/runtime.py": ModuleMaturity(
        label=STABLE,
        rationale="Tracked belief/disagreement research runtime.",
    ),
    "app/replay": ModuleMaturity(
        label=STABLE,
        rationale="Tracked replay and calibration infrastructure.",
    ),
    "app/allocation": ModuleMaturity(
        label=ACTIVE_CORE,
        rationale="Current allocation engine for the intended portfolio core.",
    ),
    "app/portfolio/runtime.py": ModuleMaturity(
        label=ACTIVE_CORE,
        rationale="Portfolio orchestrator runtime under active development, gated by explicit flag.",
    ),
    "app/portfolio/orchestrator.py": ModuleMaturity(
        label=ACTIVE_CORE,
        rationale="Portfolio orchestration logic for the intended multi-sleeve core.",
    ),
    "app/strategies/trend": ModuleMaturity(
        label=ACTIVE_CORE,
        rationale="Trend sleeve used by the orchestrator core.",
    ),
    "app/options": ModuleMaturity(
        label=EXPERIMENTAL,
        rationale="Experimental options research and execution support.",
    ),
    "app/strategies/convexity": ModuleMaturity(
        label=EXPERIMENTAL,
        rationale="Experimental convexity sleeve with active package-based surface.",
    ),
    "app/brokers/ibkr_trend_adapter.py": ModuleMaturity(
        label=EXPERIMENTAL,
        rationale="Experimental live broker adapter; not repo-defining health.",
    ),
    "app/strategies/convexity_engine.py": ModuleMaturity(
        label=DEPRECATED,
        rationale="Deprecated wrapper retained only for the package-based convexity surface, not old interface compatibility.",
    ),
    "tests/test_convexity_engine.py": ModuleMaturity(
        label=DEPRECATED,
        rationale="Legacy test surface for removed pre-package convexity API.",
    ),
}
