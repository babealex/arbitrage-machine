from __future__ import annotations

import json
from datetime import datetime, timezone

from app.common.runtime_health import assert_runtime_headroom, run_startup_checks, write_runtime_status
from app.config import Settings


def test_startup_checks_detect_low_disk_when_enforced(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("DB_PATH", str(tmp_path / "data" / "trading.db"))
    monkeypatch.setenv("LOG_PATH", str(tmp_path / "logs" / "app.log"))
    monkeypatch.setenv("HEARTBEAT_PATH", str(tmp_path / "data" / "heartbeat.txt"))
    monkeypatch.setenv("RUNTIME_STATUS_PATH", str(tmp_path / "data" / "runtime_status.json"))
    monkeypatch.setenv("REPORT_PATH", str(tmp_path / "data" / "report.json"))
    monkeypatch.setenv("EXTERNAL_CACHE_PATH", str(tmp_path / "data" / "cache.json"))
    monkeypatch.setenv("MINIMUM_FREE_DISK_BYTES", "1000")
    monkeypatch.setenv("ENFORCE_DISK_HEADROOM", "true")
    settings = Settings()
    settings.ensure_directories()

    class _Usage:
        free = 10
        total = 100

    monkeypatch.setattr("app.common.runtime_health.shutil.disk_usage", lambda _: _Usage)
    result = run_startup_checks(settings, "event_driven")

    assert not result.ok
    assert "disk_headroom_below_threshold" in result.errors


def test_write_runtime_status_updates_success_and_error_fields(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("DB_PATH", str(tmp_path / "data" / "trading.db"))
    monkeypatch.setenv("LOG_PATH", str(tmp_path / "logs" / "app.log"))
    monkeypatch.setenv("HEARTBEAT_PATH", str(tmp_path / "data" / "heartbeat.txt"))
    monkeypatch.setenv("RUNTIME_STATUS_PATH", str(tmp_path / "data" / "runtime_status.json"))
    monkeypatch.setenv("REPORT_PATH", str(tmp_path / "data" / "report.json"))
    monkeypatch.setenv("EXTERNAL_CACHE_PATH", str(tmp_path / "data" / "cache.json"))
    settings = Settings()
    settings.ensure_directories()

    start = datetime(2026, 3, 22, 15, 0, 0, tzinfo=timezone.utc)
    running = write_runtime_status(
        settings,
        "event_driven",
        "running",
        details={"cycle": {"news_items_seen": 3}},
        update_heartbeat=True,
        now=start,
    )
    assert running["last_successful_cycle_utc"] == start.isoformat()
    assert settings.heartbeat_path.read_text(encoding="utf-8") == str(int(start.timestamp()))

    later = datetime(2026, 3, 22, 15, 5, 0, tzinfo=timezone.utc)
    errored = write_runtime_status(
        settings,
        "event_driven",
        "error",
        message="disk_headroom_below_threshold",
        details={"runtime": "event_driven"},
        now=later,
    )
    payload = json.loads(settings.runtime_status_path.read_text(encoding="utf-8"))
    assert errored["last_error_message"] == "disk_headroom_below_threshold"
    assert payload["last_successful_cycle_utc"] == start.isoformat()
    assert payload["last_error_utc"] == later.isoformat()


def test_assert_runtime_headroom_returns_details(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("DB_PATH", str(tmp_path / "data" / "trading.db"))
    monkeypatch.setenv("MINIMUM_FREE_DISK_BYTES", "1")
    settings = Settings()

    class _Usage:
        free = 4096
        total = 8192

    monkeypatch.setattr("app.common.runtime_health.shutil.disk_usage", lambda _: _Usage)
    details = assert_runtime_headroom(settings)

    assert details["free_bytes"] == 4096
    assert details["total_bytes"] == 8192


def test_startup_checks_include_kalshi_and_trend_runtime_details(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("DB_PATH", str(tmp_path / "data" / "trading.db"))
    monkeypatch.setenv("LOG_PATH", str(tmp_path / "logs" / "app.log"))
    monkeypatch.setenv("HEARTBEAT_PATH", str(tmp_path / "data" / "heartbeat.txt"))
    monkeypatch.setenv("RUNTIME_STATUS_PATH", str(tmp_path / "data" / "runtime_status.json"))
    monkeypatch.setenv("REPORT_PATH", str(tmp_path / "data" / "report.json"))
    monkeypatch.setenv("FIRST_MEASUREMENT_REPORT_PATH", str(tmp_path / "data" / "first_measurement.json"))
    monkeypatch.setenv("KALSHI_CONNECTIVITY_REPORT_PATH", str(tmp_path / "data" / "kalshi_connectivity.json"))
    monkeypatch.setenv("KALSHI_PRIVATE_KEY_PATH", str(tmp_path / "missing_key.pem"))
    settings = Settings()
    result = run_startup_checks(settings, "two_sleeve_paper")

    assert "kalshi_private_key_path_not_found" in result.warnings
    assert result.details["kalshi"]["connectivity_report_path"].endswith("kalshi_connectivity.json")
    assert result.details["kalshi"]["private_key_exists"] is False
    assert result.details["trend"]["first_measurement_report_path"].endswith("first_measurement.json")
    assert result.details["trend"]["strategy_transparency_pack_path"].endswith("strategy_transparency_pack.json")
    assert result.details["trend"]["weight_mode"] == "gentle_score"
    assert result.details["trend"]["symbols"] == ["SPY", "QQQ", "IWM", "EFA", "EEM", "TLT", "IEF", "TIP", "LQD", "HYG", "GLD", "DBC"]
    assert result.details["doctrine"]["operating_hierarchy"] == ["trend", "kalshi_event"]
    assert "SPY/QQQ allocator" in result.details["doctrine"]["benchmark_question"]
