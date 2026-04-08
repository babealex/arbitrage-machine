from __future__ import annotations

"""Deprecated wrapper for the package-based convexity engine.

This module is not a compatibility shim for the removed pre-package convexity
API. Old symbols such as ``ConvexityEngineConfig`` are intentionally retired.
Callers should import from ``app.strategies.convexity.*`` directly.
"""

from app.strategies.convexity.analysis import compare_expected_vs_realized, summarize_convexity_outcomes
from app.strategies.convexity.lifecycle import (
    ConvexityCycleResult,
    ConvexityRunSummary,
    export_allocator_candidates,
    run_convexity_cycle,
    update_paper_convexity_outcomes,
)
from app.strategies.convexity.models import (
    ConvexityAccountState,
    ConvexityOpportunity,
    ConvexTradeCandidate,
    ConvexTradeOutcome,
    OptionContractQuote,
)

__all__ = [
    "ConvexityAccountState",
    "ConvexityCycleResult",
    "ConvexityOpportunity",
    "ConvexityRunSummary",
    "ConvexTradeCandidate",
    "ConvexTradeOutcome",
    "OptionContractQuote",
    "compare_expected_vs_realized",
    "export_allocator_candidates",
    "run_convexity_cycle",
    "summarize_convexity_outcomes",
    "update_paper_convexity_outcomes",
]
