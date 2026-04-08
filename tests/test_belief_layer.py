from __future__ import annotations

from app.models import Market
from app.research.belief_objects import build_kalshi_belief_snapshots
from app.research.cross_market_ingest import ExternalProxyIngestor
from app.research.disagreement_engine import DisagreementEngine


def test_build_kalshi_belief_snapshots_filters_family() -> None:
    market = Market(
        ticker="KXECONSTATCPI-26MAR-T1.0",
        event_ticker="KXECONSTATCPI-26MAR",
        title="CPI month-over-month in Mar 2026?",
        subtitle="",
        status="open",
        yes_bid=20,
        yes_ask=30,
        metadata={"quote_diagnostics": {"best_yes_bid_size": 100, "best_yes_ask_size": 120}},
    )
    rows = build_kalshi_belief_snapshots([market], ["KXECONSTATCPI"])
    assert len(rows) == 1
    assert rows[0]["source_key"] == "kalshi_event_market"
    assert rows[0]["source_type"] == "kalshi"
    assert rows[0]["object_type"] == "event_market_probability"
    assert rows[0]["comparison_metric"] == "probability"
    assert rows[0]["value"] == 0.25


def test_external_proxy_ingestor_returns_placeholders_when_fetch_disabled() -> None:
    ingestor = ExternalProxyIngestor()
    snapshot = ingestor.fetch(enabled=False)
    assert len(snapshot.rows) == 2
    assert all(row["metadata_json"].get("placeholder") for row in snapshot.rows)


def test_disagreement_engine_builds_rows_above_threshold() -> None:
    engine = DisagreementEngine(threshold=0.2)
    belief_rows = [
        {
            "timestamp_utc": "2026-03-18T00:00:00+00:00",
            "source_key": "kalshi_event_market",
            "event_key": "KXECONSTATCPI-26MAR",
            "normalized_underlying_key": "macro:inflation",
            "family": "KXECONSTATCPI",
            "event_ticker": "KXECONSTATCPI-26MAR",
            "market_ticker": "KXECONSTATCPI-26MAR-T1.0",
            "source_type": "kalshi",
            "object_type": "event_market_probability",
            "comparison_metric": "probability",
            "value": 0.1,
            "cost_estimate_bps": 35.0,
            "quality_flags_json": {},
            "belief_summary_json": {},
            "metadata_json": {},
        }
    ]
    external_rows = [
        {
            "timestamp_utc": "2026-03-18T00:00:00+00:00",
            "source_key": "fed_futures",
            "event_key": "KXECONSTATCPI-26MAR",
            "normalized_underlying_key": "macro:inflation",
            "family": "FED",
            "event_ticker": "fed:2026-03",
            "market_ticker": "fed:2026-03",
            "source_type": "fed_futures",
            "object_type": "policy_path_summary",
            "comparison_metric": "probability",
            "value": 0.8,
            "cost_estimate_bps": 8.0,
            "quality_flags_json": {},
            "belief_summary_json": {},
            "metadata_json": {},
        }
    ]
    result = engine.build(belief_rows, external_rows)
    assert len(result.rows) == 1
    assert result.rows[0]["event_group"] == "macro:inflation:KXECONSTATCPI-26MAR"
    assert result.rows[0]["tradeable"] is True
    assert result.rows[0]["net_disagreement"] > 0.2
