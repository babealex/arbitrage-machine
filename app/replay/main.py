from __future__ import annotations

import argparse
from pathlib import Path

from app.classification.service import EventClassifier
from app.config import Settings, load_dotenv
from app.logging_setup import setup_logging
from app.mapping.engine import EventTradeMapper
from app.replay.data_store import DEFAULT_RESEARCH_DATA_DIR
from app.replay.edge_analysis import ReplayEdgeAnalyzer
from app.replay.engine import ReplayEngine
from app.replay.features import ReplayFeatureExtractor
from app.replay.market_data import HistoricalMarketData
from app.replay.news_loader import HistoricalNewsLoader
from app.replay.outcomes import ReplayOutcomeDatasetBuilder
from app.replay.report import ReplayReportWriter
from app.replay.simulator import ReplayTradeSimulator


def main() -> None:
    load_dotenv()
    settings = Settings()
    parser = argparse.ArgumentParser(description="Replay event-driven trades against historical news and price data.")
    parser.add_argument("--json", dest="json_path")
    parser.add_argument("--csv", dest="csv_path")
    parser.add_argument("--from", dest="from_iso")
    parser.add_argument("--to", dest="to_iso")
    parser.add_argument("--query", dest="query", default="markets OR inflation OR fed OR oil OR earnings")
    parser.add_argument("--output", dest="output_path", default="C:/Users/alexa/arbitrage-machine/data/event_replay_report.json")
    parser.add_argument("--capital", dest="capital", type=float, default=settings.event_paper_starting_capital)
    parser.add_argument("--risk-fraction", dest="risk_fraction", type=float, default=settings.event_max_risk_per_trade_pct)
    parser.add_argument("--data-dir", dest="data_dir", default=str(DEFAULT_RESEARCH_DATA_DIR))
    parser.add_argument("--cache-dir", dest="cache_dir")
    parser.add_argument("--no-cache", dest="no_cache", action="store_true")
    parser.add_argument("--refresh-cache", dest="refresh_cache", action="store_true")
    parser.add_argument("--refresh-data", dest="refresh_data", action="store_true")
    parser.add_argument("--cache-only", dest="cache_only", action="store_true")
    parser.add_argument("--ephemeral", dest="ephemeral", action="store_true")
    parser.add_argument("--analysis-output-dir", dest="analysis_output_dir", default="C:/Users/alexa/arbitrage-machine/data/research/outcomes")
    parser.add_argument("--write-outcomes", dest="write_outcomes", action="store_true")
    parser.add_argument("--no-write-outcomes", dest="write_outcomes", action="store_false")
    parser.add_argument("--write-edge-analysis", dest="write_edge_analysis", action="store_true")
    parser.add_argument("--no-write-edge-analysis", dest="write_edge_analysis", action="store_false")
    parser.add_argument("--min-samples", dest="min_samples", type=int, default=5)
    parser.set_defaults(write_outcomes=True, write_edge_analysis=True)
    args = parser.parse_args()

    logger = setup_logging(settings)
    loader = HistoricalNewsLoader(newsapi_key=settings.event_newsapi_key)
    news_items = loader.load(
        json_path=args.json_path,
        csv_path=args.csv_path,
        from_iso=args.from_iso,
        to_iso=args.to_iso,
        query=args.query,
    )
    data_dir = Path(args.cache_dir) if args.cache_dir else Path(args.data_dir)
    classifier = EventClassifier(
        enable_openai=settings.enable_openai_classification,
        openai_api_key=settings.openai_api_key,
        openai_model=settings.openai_model,
    )
    market_data = HistoricalMarketData(
        data_dir=data_dir,
        persist=not (args.no_cache or args.ephemeral),
    )
    engine = ReplayEngine(
        classifier=classifier,
        mapper=EventTradeMapper(),
        market_data=market_data,
        simulator=ReplayTradeSimulator(capital=args.capital, risk_fraction=args.risk_fraction),
        confirmation_needed=settings.event_confirmation_min_agreeing_signals,
        scale_steps=settings.event_scale_steps,
        scale_interval_seconds=settings.event_scale_interval_seconds,
        min_severity=settings.event_min_severity,
        refresh_data=args.refresh_data or args.refresh_cache,
        cache_only=args.cache_only,
    )
    results = engine.run(news_items)
    analysis_output_dir = Path(args.analysis_output_dir)
    feature_extractor = ReplayFeatureExtractor(
        market_data=market_data,
        refresh=args.refresh_data or args.refresh_cache,
        cache_only=args.cache_only,
    )
    outcome_builder = ReplayOutcomeDatasetBuilder(feature_extractor)
    outcome_rows, outcome_summary = outcome_builder.build(results)
    outcome_paths = outcome_builder.write(outcome_rows, analysis_output_dir) if args.write_outcomes else {}
    edge_analysis = {}
    edge_analysis_path = None
    if args.write_edge_analysis:
        analyzer = ReplayEdgeAnalyzer(min_samples=args.min_samples)
        edge_analysis = analyzer.analyze(outcome_rows)
        edge_analysis_path = analyzer.write(edge_analysis, analysis_output_dir)
    stats = market_data.research_data_stats
    report = ReplayReportWriter(Path(args.output_path)).write(
        results,
        {
            "research_data_stats": {
                **stats,
                "provider_fetches_this_run": stats.get("provider_fetch_count", 0),
                "provider_fetches_lifetime": None,
                "full_store_hits_this_run": stats.get("full_store_hits", 0),
                "partial_store_hits_this_run": stats.get("partial_fetches", 0),
                "granularity_downgrade_count": stats.get("granularity_downgrade_count", 0),
            },
            "cache_stats": stats,
            "cache_enabled": not args.no_cache,
            "cache_only": args.cache_only,
            "refresh_cache": args.refresh_cache,
            "refresh_data": args.refresh_data,
            "data_dir": str(data_dir),
            "ephemeral": args.ephemeral,
            "provider_fetches_this_run": stats.get("provider_fetch_count", 0),
            "provider_fetches_lifetime": None,
            "full_store_hits_this_run": stats.get("full_store_hits", 0),
            "partial_store_hits_this_run": stats.get("partial_fetches", 0),
            "granularity_downgrade_count": stats.get("granularity_downgrade_count", 0),
            "outcome_dataset": {**outcome_summary, **outcome_paths},
            "edge_analysis": {
                "path": edge_analysis_path,
                "candidate_rule_count": edge_analysis.get("candidate_rule_count", 0),
            },
            "run_manifest": {
                "news_item_count": len(news_items),
                "output_path": args.output_path,
                "analysis_output_dir": str(analysis_output_dir),
                "data_dir": str(data_dir),
                "cache_only": args.cache_only,
                "refresh_data": bool(args.refresh_data or args.refresh_cache),
                "ephemeral": args.ephemeral,
                "capital": args.capital,
                "risk_fraction": args.risk_fraction,
                "min_samples": args.min_samples,
                "write_outcomes": args.write_outcomes,
                "write_edge_analysis": args.write_edge_analysis,
                "replay_anchoring_policy": {
                    "entry": "first_bar_at_or_after_event",
                    "exit": "first_bar_at_or_after_entry_plus_horizon",
                    "interval_request_policy": "always_request_1m_then_record_provider_downgrade",
                },
            },
        },
    )
    logger.info(
        "event_replay_complete",
        extra={
            "event": {
                "news_items_loaded": len(news_items),
                "results_produced": len(results),
                "confirmed_trades": report["summary"]["total_trades"],
                "output_path": args.output_path,
                "research_data_stats": stats,
                "outcome_dataset": outcome_paths,
                "edge_analysis_path": edge_analysis_path,
            }
        },
    )


if __name__ == "__main__":
    main()
