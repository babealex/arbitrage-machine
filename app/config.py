from __future__ import annotations

import os
import shutil
from dataclasses import dataclass, field
from pathlib import Path


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
    external_cache_path: Path = field(default_factory=lambda: Path(os.getenv("EXTERNAL_CACHE_PATH", "./data/external_fed_cache.json")))
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
        self.external_cache_path.parent.mkdir(parents=True, exist_ok=True)

    def validate(self) -> list[str]:
        warnings: list[str] = []
        if self.live_trading and (not self.kalshi_api_key_id or not self.kalshi_private_key_path):
            warnings.append("live_trading_requested_without_kalshi_auth")
        if self.min_depth <= 0:
            warnings.append("min_depth_non_positive")
        if self.poll_interval_seconds <= 0:
            warnings.append("poll_interval_non_positive")
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
