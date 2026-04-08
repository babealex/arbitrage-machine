from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from typing import Any

from app.execution.models import OrderIntent


@dataclass(slots=True)
class NewsEventRecord:
    external_id: str
    timestamp_utc: str
    source: str
    headline: str
    body: str | None = None
    url: str | None = None
    metadata_json: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ClassifiedEventRecord:
    news_event_id: int
    event_type: str
    region: str
    severity: float
    directional_bias: str
    assets_affected: list[str] = field(default_factory=list)
    metadata_json: dict[str, Any] = field(default_factory=dict)
    created_at: str = ""


@dataclass(slots=True)
class TradeCandidateRecord:
    classified_event_id: int
    symbol: str
    side: str
    event_type: str
    status: str
    confidence: float
    confirmation_score: float
    metadata_json: dict[str, Any] = field(default_factory=dict)
    created_at: str = ""


@dataclass(slots=True)
class EventEntryQueueRecord:
    trade_candidate_id: int
    due_at: str
    symbol: str
    side: str
    quantity: int
    tranche_index: int
    total_tranches: int
    status: str
    metadata_json: dict[str, Any] = field(default_factory=dict)
    created_at: str = ""


@dataclass(slots=True)
class EventPositionRecord:
    trade_candidate_id: int
    news_event_id: int
    symbol: str
    side: str
    quantity: int
    entry_price: float
    stop_price: float
    opened_at: str
    planned_exit_at: str
    status: str
    metadata_json: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class EventTradeOutcomeRecord:
    position_id: int
    symbol: str
    entry_price: float
    exit_price: float
    return_pct: float
    holding_seconds: float
    exit_reason: str
    metadata_json: dict[str, Any] = field(default_factory=dict)
    closed_at: str = ""


@dataclass(slots=True)
class KalshiOrderRecord:
    intent: OrderIntent
    status: str
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: str | None = None


@dataclass(slots=True)
class MonotonicityObservationRecord:
    signal_key: str
    ticker: str
    lower_ticker: str
    higher_ticker: str
    product_family: str
    accepted_at: str
    edge_before_fees: float
    edge_after_fees: float
    lower_yes_ask_price: int | None = None
    lower_yes_ask_size: int = 0
    higher_yes_bid_price: int | None = None
    higher_yes_bid_size: int = 0
    next_refresh_status: str | None = None
    next_refresh_observed_at: str | None = None
    time_to_disappear_seconds: float | None = None
    latest_status: str | None = None
    latest_observed_at: str | None = None
    metadata_json: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class CrossMarketSnapshotRecord:
    timestamp: str
    event_ticker: str
    contract_pair: str | None
    kalshi_probability: float
    kalshi_bid: float
    kalshi_ask: float
    kalshi_yes_bid_size: int
    kalshi_yes_ask_size: int
    spy_price: float | None = None
    qqq_price: float | None = None
    tlt_price: float | None = None
    btc_price: float | None = None
    derived_risk_score: float = 0.0
    notes: str = ""


@dataclass(slots=True)
class BeliefSnapshotRecord:
    timestamp_utc: str
    family: str
    event_ticker: str
    market_ticker: str
    source_type: str
    object_type: str
    value: float
    source_key: str | None = None
    event_key: str | None = None
    normalized_underlying_key: str | None = None
    confidence: float | None = None
    comparison_metric: str | None = None
    payload_summary_json: dict[str, Any] = field(default_factory=dict)
    belief_summary_json: dict[str, Any] = field(default_factory=dict)
    quality_flags_json: dict[str, Any] = field(default_factory=dict)
    cost_estimate_bps: float | None = None
    bid: float | None = None
    ask: float | None = None
    size_bid: int | None = None
    size_ask: int | None = None
    metadata_json: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class BeliefSourceRecord:
    source_key: str
    source_type: str
    description: str
    venue: str | None = None
    instrument_class: str | None = None
    active: bool = True
    metadata_json: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class EventDefinitionRecord:
    event_key: str
    family: str
    normalized_underlying_key: str
    title: str
    event_date: str | None = None
    metadata_json: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class DisagreementSnapshotRecord:
    timestamp_utc: str
    event_group: str
    kalshi_reference: str
    external_reference: str
    disagreement_score: float
    zscore: float
    normalized_underlying_key: str | None = None
    left_source_key: str | None = None
    right_source_key: str | None = None
    gross_disagreement: float | None = None
    cost_estimate_bps: float | None = None
    net_disagreement: float | None = None
    tradeable: bool = False
    metadata_json: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class CrossMarketDisagreementRecord:
    timestamp_utc: str
    normalized_underlying_key: str
    event_key: str
    comparison_metric: str
    left_source_key: str
    right_source_key: str
    left_value: float
    right_value: float
    gross_disagreement: float
    fees_bps: float
    slippage_bps: float
    execution_uncertainty_bps: float
    net_disagreement: float
    tradeable: bool
    metadata_json: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class OutcomeTrackingRecord:
    timestamp_detected: str
    event_ticker: str
    family: str
    initial_value: float
    disagreement_score: float
    metadata_json: dict[str, Any] = field(default_factory=dict)
    due_at_30s: str | None = None
    due_at_60s: str | None = None
    due_at_300s: str | None = None
    due_at_900s: str | None = None
    value_after_30s: float | None = None
    value_after_60s: float | None = None
    value_after_300s: float | None = None
    value_after_900s: float | None = None
    max_favorable_move: float | None = None
    max_adverse_move: float | None = None
    resolved_direction: str | None = None


@dataclass(slots=True)
class OutcomeRealizationRecord:
    timestamp_utc: str
    event_key: str
    normalized_underlying_key: str
    outcome_type: str
    realized_value: float
    metadata_json: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class OutcomeTrackingUpdate:
    metadata_json: dict[str, Any] = field(default_factory=dict)
    due_at_30s: str | None = None
    due_at_60s: str | None = None
    due_at_300s: str | None = None
    due_at_900s: str | None = None
    value_after_30s: float | None = None
    value_after_60s: float | None = None
    value_after_300s: float | None = None
    value_after_900s: float | None = None
    max_favorable_move: float | None = None
    max_adverse_move: float | None = None
    resolved_direction: str | None = None


@dataclass(slots=True)
class KalshiReportingBundle:
    evaluations: list[sqlite3.Row]
    signals: list[sqlite3.Row]
    orders: list[sqlite3.Row]
    order_events: list[sqlite3.Row]
    fills: list[sqlite3.Row]
    execution_outcomes: list[sqlite3.Row]
    observations: list[sqlite3.Row]
    cross_market_snapshots: list[sqlite3.Row]
    risk_events: list[sqlite3.Row]


@dataclass(slots=True)
class BeliefReportingBundle:
    belief_sources: list[sqlite3.Row]
    event_definitions: list[sqlite3.Row]
    belief_snapshots: list[sqlite3.Row]
    disagreement_snapshots: list[sqlite3.Row]
    cross_market_disagreements: list[sqlite3.Row]
    outcome_realizations: list[sqlite3.Row]
    outcome_tracking: list[sqlite3.Row]


@dataclass(slots=True)
class EventReportingBundle:
    news_events: list[sqlite3.Row]
    classified_events: list[sqlite3.Row]
    trade_candidates: list[sqlite3.Row]
    entry_queue: list[sqlite3.Row]
    positions: list[sqlite3.Row]
    trade_outcomes: list[sqlite3.Row]
    paper_execution_audit: list[sqlite3.Row]


@dataclass(slots=True)
class PortfolioCycleSnapshotRecord:
    run_id: str
    observed_at: str
    candidate_count: int
    approved_count: int
    rejected_count: int
    allocated_capital_decimal: str
    expected_portfolio_ev_decimal: str
    strategy_counts_json: dict[str, int] = field(default_factory=dict)
    rejection_counts_json: dict[str, int] = field(default_factory=dict)
    readiness_json: dict[str, Any] = field(default_factory=dict)
