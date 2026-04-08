from __future__ import annotations

import os
import shutil
from dataclasses import dataclass, field
from pathlib import Path

DEFAULT_TREND_SYMBOLS = "SPY,QQQ,IWM,EFA,EEM,TLT,IEF,TIP,LQD,HYG,GLD,DBC"


def load_dotenv(path: str = ".env") -> None:
    env_path = Path(path)
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def _get_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _get_float(name: str, default: float) -> float:
    value = os.getenv(name)
    return float(value) if value is not None else default


def _get_int(name: str, default: int) -> int:
    value = os.getenv(name)
    return int(value) if value is not None else default


def _get_csv(name: str, default: str) -> list[str]:
    value = os.getenv(name, default)
    return [item.strip().lower() for item in value.split(",") if item.strip()]


@dataclass(slots=True)
class Settings:
    app_env: str = field(default_factory=lambda: os.getenv("APP_ENV", "dev"))
    app_mode: str = field(default_factory=lambda: os.getenv("APP_MODE", "safe"))
    live_trading: bool = field(default_factory=lambda: _get_bool("LIVE_TRADING", False))
    log_level: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))
    db_path: Path = field(default_factory=lambda: Path(os.getenv("DB_PATH", "./data/trading.db")))
    log_path: Path = field(default_factory=lambda: Path(os.getenv("LOG_PATH", "./logs/app.log")))
    heartbeat_path: Path = field(default_factory=lambda: Path(os.getenv("HEARTBEAT_PATH", "./data/heartbeat.txt")))
    runtime_status_path: Path = field(default_factory=lambda: Path(os.getenv("RUNTIME_STATUS_PATH", "./data/runtime_status.json")))
    report_path: Path = field(default_factory=lambda: Path(os.getenv("REPORT_PATH", "./data/paper_report.json")))
    first_measurement_report_path: Path = field(default_factory=lambda: Path(os.getenv("FIRST_MEASUREMENT_REPORT_PATH", "./data/first_measurement_report.json")))
    multi_cycle_research_report_path: Path = field(default_factory=lambda: Path(os.getenv("MULTI_CYCLE_RESEARCH_REPORT_PATH", "./data/multi_cycle_research_report.json")))
    trend_replay_diversity_report_path: Path = field(default_factory=lambda: Path(os.getenv("TREND_REPLAY_DIVERSITY_REPORT_PATH", "./data/trend_replay_diversity_report.json")))
    trend_threshold_sensitivity_report_path: Path = field(default_factory=lambda: Path(os.getenv("TREND_THRESHOLD_SENSITIVITY_REPORT_PATH", "./data/trend_threshold_sensitivity_report.json")))
    strategy_transparency_pack_path: Path = field(default_factory=lambda: Path(os.getenv("STRATEGY_TRANSPARENCY_PACK_PATH", "./data/strategy_transparency_pack.json")))
    dashboard_state_path: Path = field(default_factory=lambda: Path(os.getenv("DASHBOARD_STATE_PATH", "./data/control_panel_state.json")))
    kalshi_connectivity_report_path: Path = field(default_factory=lambda: Path(os.getenv("KALSHI_CONNECTIVITY_REPORT_PATH", "./data/kalshi_connectivity_report.json")))
    external_cache_path: Path = field(default_factory=lambda: Path(os.getenv("EXTERNAL_CACHE_PATH", "./data/external_fed_cache.json")))
    control_panel_host: str = field(default_factory=lambda: os.getenv("CONTROL_PANEL_HOST", "127.0.0.1"))
    control_panel_port: int = field(default_factory=lambda: _get_int("CONTROL_PANEL_PORT", 8765))
    minimum_free_disk_bytes: int = field(default_factory=lambda: _get_int("MINIMUM_FREE_DISK_BYTES", 268435456))
    enforce_disk_headroom: bool = field(default_factory=lambda: _get_bool("ENFORCE_DISK_HEADROOM", True))
    kalshi_api_url: str = field(default_factory=lambda: os.getenv("KALSHI_API_URL", "https://api.elections.kalshi.com/trade-api/v2"))
    kalshi_ws_url: str = field(default_factory=lambda: os.getenv("KALSHI_WS_URL", "wss://api.elections.kalshi.com/trade-api/ws/v2"))
    kalshi_api_key_id: str = field(default_factory=lambda: os.getenv("KALSHI_API_KEY_ID", ""))
    kalshi_private_key_path: str = field(default_factory=lambda: os.getenv("KALSHI_PRIVATE_KEY_PATH", ""))
    kalshi_passphrase: str = field(default_factory=lambda: os.getenv("KALSHI_PASSPHRASE", ""))
    fedwatch_url: str = field(default_factory=lambda: os.getenv("FEDWATCH_URL", "https://www.cmegroup.com/markets/interest-rates/cme-fedwatch-tool.html"))
    fedwatch_max_staleness_seconds: int = field(default_factory=lambda: _get_int("FEDWATCH_MAX_STALENESS_SECONDS", 600))
    alert_webhook_url: str = field(default_factory=lambda: os.getenv("ALERT_WEBHOOK_URL", ""))
    alert_email_from: str = field(default_factory=lambda: os.getenv("ALERT_EMAIL_FROM", ""))
    alert_email_to: str = field(default_factory=lambda: os.getenv("ALERT_EMAIL_TO", ""))
    smtp_host: str = field(default_factory=lambda: os.getenv("SMTP_HOST", ""))
    smtp_port: int = field(default_factory=lambda: _get_int("SMTP_PORT", 587))
    smtp_username: str = field(default_factory=lambda: os.getenv("SMTP_USERNAME", ""))
    smtp_password: str = field(default_factory=lambda: os.getenv("SMTP_PASSWORD", ""))
    poll_interval_seconds: int = field(default_factory=lambda: _get_int("POLL_INTERVAL_SECONDS", 5))
    portfolio_cycle_interval_seconds: int = field(default_factory=lambda: _get_int("PORTFOLIO_CYCLE_INTERVAL_SECONDS", 60))
    enable_two_sleeve_runtime: bool = field(default_factory=lambda: _get_bool("ENABLE_TWO_SLEEVE_RUNTIME", True))
    enable_portfolio_orchestrator: bool = field(default_factory=lambda: _get_bool("ENABLE_PORTFOLIO_ORCHESTRATOR", False))
    enable_trend_sleeve: bool = field(default_factory=lambda: _get_bool("ENABLE_TREND_SLEEVE", True))
    enable_kalshi_sleeve: bool = field(default_factory=lambda: _get_bool("ENABLE_KALSHI_SLEEVE", True))
    enable_options_sleeve: bool = field(default_factory=lambda: _get_bool("ENABLE_OPTIONS_SLEEVE", False))
    enable_convexity_sleeve: bool = field(default_factory=lambda: _get_bool("ENABLE_CONVEXITY_SLEEVE", False))
    total_paper_capital: float = field(default_factory=lambda: _get_float("TOTAL_PAPER_CAPITAL", 100000.0))
    trend_capital_weight_default: float = field(default_factory=lambda: _get_float("TREND_CAPITAL_WEIGHT_DEFAULT", 0.75))
    kalshi_capital_weight_default: float = field(default_factory=lambda: _get_float("KALSHI_CAPITAL_WEIGHT_DEFAULT", 0.25))
    trend_data_mode: str = field(default_factory=lambda: os.getenv("TREND_DATA_MODE", "auto").strip().lower())
    trend_replay_advance_mode: str = field(default_factory=lambda: os.getenv("TREND_REPLAY_ADVANCE_MODE", "static").strip().lower())
    trend_bootstrap_variant: str = field(default_factory=lambda: os.getenv("TREND_BOOTSTRAP_VARIANT", "smooth").strip().lower())
    trend_weight_mode: str = field(default_factory=lambda: os.getenv("TREND_WEIGHT_MODE", "gentle_score").strip().lower())
    trend_min_rebalance_fraction: float = field(default_factory=lambda: _get_float("TREND_MIN_REBALANCE_FRACTION", 0.10))
    kalshi_data_mode: str = field(default_factory=lambda: os.getenv("KALSHI_DATA_MODE", "auto").strip().lower())
    trend_symbols_raw: str = field(default_factory=lambda: os.getenv("TREND_SYMBOLS", DEFAULT_TREND_SYMBOLS))
    trend_fetch_retries: int = field(default_factory=lambda: _get_int("TREND_FETCH_RETRIES", 2))
    trend_fetch_backoff_seconds: float = field(default_factory=lambda: _get_float("TREND_FETCH_BACKOFF_SECONDS", 1.0))
    trend_fetch_timeout_seconds: int = field(default_factory=lambda: _get_int("TREND_FETCH_TIMEOUT_SECONDS", 10))
    kalshi_request_timeout_seconds: int = field(default_factory=lambda: _get_int("KALSHI_REQUEST_TIMEOUT_SECONDS", 15))
    kalshi_request_retries: int = field(default_factory=lambda: _get_int("KALSHI_REQUEST_RETRIES", 2))
    kalshi_request_backoff_seconds: float = field(default_factory=lambda: _get_float("KALSHI_REQUEST_BACKOFF_SECONDS", 1.0))
    live_enabled_sleeves_raw: list[str] = field(default_factory=lambda: _get_csv("LIVE_ENABLED_SLEEVES", "kalshi"))
    trend_live_broker_mode: str = field(default_factory=lambda: os.getenv("TREND_LIVE_BROKER_MODE", "none").strip().lower())
    trend_live_broker_enabled: bool = field(default_factory=lambda: _get_bool("TREND_LIVE_BROKER_ENABLED", False))
    ibkr_host: str = field(default_factory=lambda: os.getenv("IBKR_HOST", "127.0.0.1"))
    ibkr_port: int = field(default_factory=lambda: _get_int("IBKR_PORT", 7497))
    ibkr_client_id: int = field(default_factory=lambda: _get_int("IBKR_CLIENT_ID", 0))
    ibkr_account_id: str = field(default_factory=lambda: os.getenv("IBKR_ACCOUNT_ID", ""))
    max_capital_kalshi: float = field(default_factory=lambda: _get_float("MAX_CAPITAL_KALSHI", 0.30))
    max_capital_trend: float = field(default_factory=lambda: _get_float("MAX_CAPITAL_TREND", 0.60))
    max_capital_options: float = field(default_factory=lambda: _get_float("MAX_CAPITAL_OPTIONS", 0.10))
    timing_experiment_mode: bool = field(default_factory=lambda: _get_bool("TIMING_EXPERIMENT_MODE", False))
    timing_confirmation_delay_seconds: float = field(default_factory=lambda: _get_float("TIMING_CONFIRMATION_DELAY_SECONDS", 1.0))
    timing_followup_delay_seconds: float = field(default_factory=lambda: _get_float("TIMING_FOLLOWUP_DELAY_SECONDS", 1.0))
    max_daily_loss_pct: float = field(default_factory=lambda: _get_float("MAX_DAILY_LOSS_PCT", 0.02))
    max_total_exposure_pct: float = field(default_factory=lambda: _get_float("MAX_TOTAL_EXPOSURE_PCT", 0.30))
    max_position_per_trade_pct: float = field(default_factory=lambda: _get_float("MAX_POSITION_PER_TRADE_PCT", 0.02))
    max_failed_trades_per_day: int = field(default_factory=lambda: _get_int("MAX_FAILED_TRADES_PER_DAY", 2))
    slippage_buffer_bps: float = field(default_factory=lambda: _get_float("SLIPPAGE_BUFFER_BPS", 35))
    min_edge_bps: float = field(default_factory=lambda: _get_float("MIN_EDGE_BPS", 500))
    min_edge: float = field(default_factory=lambda: _get_float("MIN_EDGE", 0.02))
    min_depth: int = field(default_factory=lambda: _get_int("MIN_DEPTH", 10))
    monotonicity_min_top_level_depth: int = field(default_factory=lambda: _get_int("MONOTONICITY_MIN_TOP_LEVEL_DEPTH", 50))
    monotonicity_min_size_multiple: float = field(default_factory=lambda: _get_float("MONOTONICITY_MIN_SIZE_MULTIPLE", 2.0))
    monotonicity_ladder_cooldown_seconds: int = field(default_factory=lambda: _get_int("MONOTONICITY_LADDER_COOLDOWN_SECONDS", 15))
    enable_cross_market_mode: bool = field(default_factory=lambda: _get_bool("ENABLE_CROSS_MARKET_MODE", True))
    cross_market_families: str = field(default_factory=lambda: os.getenv("CROSS_MARKET_FAMILIES", "KXECONSTATCPI,KXECONSTATCPICORE,KX3MTBILL"))
    cross_market_poll_interval_seconds: int = field(default_factory=lambda: _get_int("CROSS_MARKET_POLL_INTERVAL_SECONDS", 5))
    enable_belief_layer: bool = field(default_factory=lambda: _get_bool("ENABLE_BELIEF_LAYER", False))
    belief_layer_only_mode: bool = field(default_factory=lambda: _get_bool("BELIEF_LAYER_ONLY_MODE", False))
    belief_families: str = field(default_factory=lambda: os.getenv("BELIEF_FAMILIES", "KXECONSTATCPI,KXECONSTATCPICORE,KX3MTBILL"))
    belief_poll_interval_seconds: int = field(default_factory=lambda: _get_int("BELIEF_POLL_INTERVAL_SECONDS", 5))
    disagreement_threshold: float = field(default_factory=lambda: _get_float("DISAGREEMENT_THRESHOLD", 0.20))
    enable_external_proxy_fetch: bool = field(default_factory=lambda: _get_bool("ENABLE_EXTERNAL_PROXY_FETCH", True))
    convexity_enabled: bool = field(default_factory=lambda: _get_bool("CONVEXITY_ENABLED", _get_bool("ENABLE_CONVEXITY_SLEEVE", False)))
    convexity_max_risk_per_trade_pct: float = field(default_factory=lambda: _get_float("CONVEXITY_MAX_RISK_PER_TRADE_PCT", 0.02))
    convexity_max_total_exposure_pct: float = field(default_factory=lambda: _get_float("CONVEXITY_MAX_TOTAL_EXPOSURE_PCT", 0.20))
    convexity_min_reward_to_risk: float = field(default_factory=lambda: _get_float("CONVEXITY_MIN_REWARD_TO_RISK", 5.0))
    convexity_min_expected_value_after_costs: float = field(default_factory=lambda: _get_float("CONVEXITY_MIN_EXPECTED_VALUE_AFTER_COSTS", 0.0))
    convexity_min_oi: int = field(default_factory=lambda: _get_int("CONVEXITY_MIN_OI", 100))
    convexity_min_volume: int = field(default_factory=lambda: _get_int("CONVEXITY_MIN_VOLUME", 10))
    convexity_max_bid_ask_pct_of_debit: float = field(default_factory=lambda: _get_float("CONVEXITY_MAX_BID_ASK_PCT_OF_DEBIT", 0.25))
    convexity_allowed_dte_min: int = field(default_factory=lambda: _get_int("CONVEXITY_ALLOWED_DTE_MIN", 7))
    convexity_allowed_dte_max: int = field(default_factory=lambda: _get_int("CONVEXITY_ALLOWED_DTE_MAX", 30))
    convexity_max_open_trades: int = field(default_factory=lambda: _get_int("CONVEXITY_MAX_OPEN_TRADES", 5))
    convexity_persistence_enabled: bool = field(default_factory=lambda: _get_bool("CONVEXITY_PERSISTENCE_ENABLED", True))
    convexity_reporting_enabled: bool = field(default_factory=lambda: _get_bool("CONVEXITY_REPORTING_ENABLED", True))
    convexity_lookback_days_summary: int = field(default_factory=lambda: _get_int("CONVEXITY_LOOKBACK_DAYS_SUMMARY", 90))
    convexity_live_execution_enabled: bool = field(default_factory=lambda: _get_bool("CONVEXITY_LIVE_EXECUTION_ENABLED", False))
    convexity_paper_execution_enabled: bool = field(default_factory=lambda: _get_bool("CONVEXITY_PAPER_EXECUTION_ENABLED", True))
    convexity_macro_events: str = field(default_factory=lambda: os.getenv("CONVEXITY_MACRO_EVENTS", ""))
    convexity_earnings_events: str = field(default_factory=lambda: os.getenv("CONVEXITY_EARNINGS_EVENTS", ""))
    enable_partition_strategy: bool = field(default_factory=lambda: _get_bool("ENABLE_PARTITION_STRATEGY", True))
    partition_min_coverage: float = field(default_factory=lambda: _get_float("PARTITION_MIN_COVERAGE", 0.98))
    min_time_to_expiry_seconds: int = field(default_factory=lambda: _get_int("MIN_TIME_TO_EXPIRY_SECONDS", 1800))
    external_mismatch_threshold: float = field(default_factory=lambda: _get_float("EXTERNAL_MISMATCH_THRESHOLD", 0.06))
    max_market_data_staleness_seconds: int = field(default_factory=lambda: _get_int("MAX_MARKET_DATA_STALENESS_SECONDS", 20))
    max_external_data_staleness_seconds: int = field(default_factory=lambda: _get_int("MAX_EXTERNAL_DATA_STALENESS_SECONDS", 600))
    external_fetch_retries: int = field(default_factory=lambda: _get_int("EXTERNAL_FETCH_RETRIES", 3))
    external_fetch_backoff_seconds: float = field(default_factory=lambda: _get_float("EXTERNAL_FETCH_BACKOFF_SECONDS", 1.5))
    external_cache_freshness_seconds: int = field(default_factory=lambda: _get_int("EXTERNAL_CACHE_FRESHNESS_SECONDS", 21600))
    allowed_market_keywords: str = field(default_factory=lambda: os.getenv("ALLOWED_MARKET_KEYWORDS", "fed,fomc,rate,cpi,inflation,macro,treasury,yield,unemployment,gdp"))
    excluded_market_keywords: str = field(default_factory=lambda: os.getenv("EXCLUDED_MARKET_KEYWORDS", "sport,game,match,team,player,championship,crosscategory,cross-category,multigame"))
    market_fetch_pages: int = field(default_factory=lambda: _get_int("MARKET_FETCH_PAGES", 5))
    diagnostic_market_sample_size: int = field(default_factory=lambda: _get_int("DIAGNOSTIC_MARKET_SAMPLE_SIZE", 20))
    macro_series_limit: int = field(default_factory=lambda: _get_int("MACRO_SERIES_LIMIT", 40))
    enable_event_driven_mode: bool = field(default_factory=lambda: _get_bool("ENABLE_EVENT_DRIVEN_MODE", False))
    event_trading_only_mode: bool = field(default_factory=lambda: _get_bool("EVENT_TRADING_ONLY_MODE", False))
    event_execution_enabled: bool = field(default_factory=lambda: _get_bool("EVENT_EXECUTION_ENABLED", False))
    event_poll_interval_seconds: int = field(default_factory=lambda: _get_int("EVENT_POLL_INTERVAL_SECONDS", 15))
    event_confirmation_min_agreeing_signals: int = field(default_factory=lambda: _get_int("EVENT_CONFIRMATION_MIN_AGREEING_SIGNALS", 2))
    event_max_risk_per_trade_pct: float = field(default_factory=lambda: _get_float("EVENT_MAX_RISK_PER_TRADE_PCT", 0.01))
    event_stop_loss_pct: float = field(default_factory=lambda: _get_float("EVENT_STOP_LOSS_PCT", 0.015))
    event_max_hold_seconds: int = field(default_factory=lambda: _get_int("EVENT_MAX_HOLD_SECONDS", 3600))
    event_scale_steps: int = field(default_factory=lambda: _get_int("EVENT_SCALE_STEPS", 2))
    event_scale_interval_seconds: int = field(default_factory=lambda: _get_int("EVENT_SCALE_INTERVAL_SECONDS", 120))
    event_paper_starting_capital: float = field(default_factory=lambda: _get_float("EVENT_PAPER_STARTING_CAPITAL", 100000.0))
    event_paper_submission_latency_seconds: float = field(default_factory=lambda: _get_float("EVENT_PAPER_SUBMISSION_LATENCY_SECONDS", 2.0))
    event_paper_half_spread_bps: float = field(default_factory=lambda: _get_float("EVENT_PAPER_HALF_SPREAD_BPS", 8.0))
    event_paper_top_level_size: int = field(default_factory=lambda: _get_int("EVENT_PAPER_TOP_LEVEL_SIZE", 25))
    event_paper_depth_levels: int = field(default_factory=lambda: _get_int("EVENT_PAPER_DEPTH_LEVELS", 3))
    event_paper_passive_recheck_seconds: float = field(default_factory=lambda: _get_float("EVENT_PAPER_PASSIVE_RECHECK_SECONDS", 1.0))
    event_paper_session_boundary_buffer_seconds: int = field(default_factory=lambda: _get_int("EVENT_PAPER_SESSION_BOUNDARY_BUFFER_SECONDS", 300))
    event_paper_session_boundary_liquidity_factor: float = field(default_factory=lambda: _get_float("EVENT_PAPER_SESSION_BOUNDARY_LIQUIDITY_FACTOR", 0.25))
    event_paper_session_boundary_spread_multiplier: float = field(default_factory=lambda: _get_float("EVENT_PAPER_SESSION_BOUNDARY_SPREAD_MULTIPLIER", 2.0))
    event_min_severity: float = field(default_factory=lambda: _get_float("EVENT_MIN_SEVERITY", 0.45))
    event_newsapi_key: str = field(default_factory=lambda: os.getenv("EVENT_NEWSAPI_KEY", ""))
    event_newsapi_url: str = field(default_factory=lambda: os.getenv("EVENT_NEWSAPI_URL", "https://newsapi.org/v2/top-headlines"))
    event_rss_feed_urls: str = field(default_factory=lambda: os.getenv("EVENT_RSS_FEED_URLS", "https://feeds.reuters.com/reuters/businessNews,https://feeds.reuters.com/news/usmarkets"))
    enable_openai_classification: bool = field(default_factory=lambda: _get_bool("ENABLE_OPENAI_CLASSIFICATION", False))
    openai_api_key: str = field(default_factory=lambda: os.getenv("OPENAI_API_KEY", ""))
    openai_model: str = field(default_factory=lambda: os.getenv("OPENAI_MODEL", "gpt-5.4-mini"))

    def ensure_directories(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self.heartbeat_path.parent.mkdir(parents=True, exist_ok=True)
        self.runtime_status_path.parent.mkdir(parents=True, exist_ok=True)
        self.report_path.parent.mkdir(parents=True, exist_ok=True)
        self.first_measurement_report_path.parent.mkdir(parents=True, exist_ok=True)
        self.strategy_transparency_pack_path.parent.mkdir(parents=True, exist_ok=True)
        self.dashboard_state_path.parent.mkdir(parents=True, exist_ok=True)
        self.kalshi_connectivity_report_path.parent.mkdir(parents=True, exist_ok=True)
        self.trend_threshold_sensitivity_report_path.parent.mkdir(parents=True, exist_ok=True)
        self.external_cache_path.parent.mkdir(parents=True, exist_ok=True)

    def validate(self) -> list[str]:
        warnings: list[str] = []
        known_live_sleeves = {"kalshi", "trend", "options"}
        if self.live_trading and (not self.kalshi_api_key_id or not self.kalshi_private_key_path):
            warnings.append("live_trading_requested_without_kalshi_auth")
        if self.kalshi_api_key_id and not self.kalshi_private_key_path:
            warnings.append("kalshi_private_key_path_missing")
        if self.kalshi_private_key_path and not self.kalshi_api_key_id:
            warnings.append("kalshi_api_key_id_missing")
        if self.kalshi_private_key_path and not Path(self.kalshi_private_key_path).exists():
            warnings.append("kalshi_private_key_path_not_found")
        invalid_live_sleeves = [sleeve for sleeve in self.live_enabled_sleeves if sleeve not in known_live_sleeves]
        if invalid_live_sleeves:
            warnings.append(f"unknown_live_enabled_sleeves:{','.join(invalid_live_sleeves)}")
        if self.trend_live_broker_mode not in {"none", "manual", "ibkr"}:
            warnings.append(f"unknown_trend_live_broker_mode:{self.trend_live_broker_mode}")
        if self.trend_live_broker_mode == "ibkr" and not self.trend_live_broker_enabled:
            warnings.append("ibkr_mode_enabled_without_trend_live_broker_enabled")
        if self.trend_live_broker_mode == "ibkr" and not self.ibkr_account_id:
            warnings.append("ibkr_account_id_missing")
        if self.live_trading and "trend" in self.live_enabled_sleeves and self.trend_live_broker_mode == "none":
            warnings.append("trend_live_requested_without_supported_broker")
        if self.live_trading and "options" in self.live_enabled_sleeves:
            warnings.append("options_live_requested_without_supported_broker")
        if self.convexity_live_execution_enabled:
            warnings.append("convexity_live_requested_without_supported_broker")
        if self.min_depth <= 0:
            warnings.append("min_depth_non_positive")
        if self.poll_interval_seconds <= 0:
            warnings.append("poll_interval_non_positive")
        if self.portfolio_cycle_interval_seconds <= 0:
            warnings.append("portfolio_cycle_interval_non_positive")
        if self.control_panel_port <= 0:
            warnings.append("control_panel_port_non_positive")
        if self.total_paper_capital <= 0:
            warnings.append("total_paper_capital_non_positive")
        if self.trend_data_mode not in {"auto", "live", "cached"}:
            warnings.append(f"unknown_trend_data_mode:{self.trend_data_mode}")
        if self.trend_weight_mode not in {"current", "soft_monotone", "rank_normalized", "gentle_score"}:
            warnings.append(f"unknown_trend_weight_mode:{self.trend_weight_mode}")
        if self.trend_min_rebalance_fraction <= 0:
            warnings.append("trend_min_rebalance_fraction_non_positive")
        if self.kalshi_data_mode not in {"auto", "live", "cached"}:
            warnings.append(f"unknown_kalshi_data_mode:{self.kalshi_data_mode}")
        if self.trend_fetch_retries < 0:
            warnings.append("trend_fetch_retries_negative")
        if self.trend_fetch_timeout_seconds <= 0:
            warnings.append("trend_fetch_timeout_non_positive")
        if self.kalshi_request_timeout_seconds <= 0:
            warnings.append("kalshi_request_timeout_non_positive")
        if self.kalshi_request_retries < 0:
            warnings.append("kalshi_request_retries_negative")
        total_weight = self.trend_capital_weight_default + self.kalshi_capital_weight_default
        if total_weight <= 0:
            warnings.append("two_sleeve_capital_weight_sum_non_positive")
        if self.enable_event_driven_mode and self.event_poll_interval_seconds <= 0:
            warnings.append("event_poll_interval_non_positive")
        if self.minimum_free_disk_bytes < 0:
            warnings.append("minimum_free_disk_bytes_negative")
        if self.minimum_free_disk_bytes > 0:
            disk_root = self.db_path.anchor or "."
            try:
                free_bytes = shutil.disk_usage(disk_root).free
                if free_bytes < self.minimum_free_disk_bytes:
                    warnings.append("disk_headroom_below_threshold")
            except OSError:
                warnings.append("disk_headroom_unavailable")
        return warnings

    @property
    def kalshi_mode(self) -> str:
        return "authenticated" if self.kalshi_api_key_id and self.kalshi_private_key_path else "data_only"

    @property
    def allowed_market_terms(self) -> list[str]:
        return [item.strip().lower() for item in self.allowed_market_keywords.split(",") if item.strip()]

    @property
    def excluded_market_terms(self) -> list[str]:
        return [item.strip().lower() for item in self.excluded_market_keywords.split(",") if item.strip()]

    @property
    def cross_market_family_terms(self) -> list[str]:
        return [item.strip() for item in self.cross_market_families.split(",") if item.strip()]

    @property
    def belief_family_terms(self) -> list[str]:
        return [item.strip() for item in self.belief_families.split(",") if item.strip()]

    @property
    def event_rss_urls(self) -> list[str]:
        return [item.strip() for item in self.event_rss_feed_urls.split(",") if item.strip()]

    @property
    def max_capital_per_strategy(self) -> dict[str, float]:
        return {
            "kalshi": self.max_capital_kalshi,
            "trend": self.max_capital_trend,
            "options": self.max_capital_options,
        }

    @property
    def two_sleeve_capital_weights(self) -> dict[str, float]:
        return {
            "trend": self.trend_capital_weight_default,
            "kalshi_event": self.kalshi_capital_weight_default,
        }

    @property
    def trend_symbols(self) -> list[str]:
        return [item.strip().upper() for item in self.trend_symbols_raw.split(",") if item.strip()]

    @property
    def live_enabled_sleeves(self) -> list[str]:
        return self.live_enabled_sleeves_raw

    @property
    def convexity_macro_event_rows(self) -> list[str]:
        return [item.strip() for item in self.convexity_macro_events.split(";") if item.strip()]

    @property
    def convexity_earnings_event_rows(self) -> list[str]:
        return [item.strip() for item in self.convexity_earnings_events.split(";") if item.strip()]
