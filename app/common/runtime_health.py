from __future__ import annotations

import json
import os
import shutil
import socket
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.config import Settings
from app.strategy_doctrine import doctrine_summary


STATUS_SCHEMA_VERSION = "runtime-status-v1"


@dataclass(slots=True)
class StartupCheckResult:
    runtime_name: str
    checked_at_utc: str
    errors: list[str]
    warnings: list[str]
    details: dict[str, Any]

    @property
    def ok(self) -> bool:
        return not self.errors


def run_startup_checks(settings: Settings, runtime_name: str) -> StartupCheckResult:
    checked_at = datetime.now(timezone.utc).isoformat()
    errors: list[str] = []
    warnings = list(settings.validate())
    writable_dirs = _unique_dirs(
        [
            settings.db_path.parent,
            settings.log_path.parent,
            settings.heartbeat_path.parent,
            settings.runtime_status_path.parent,
            settings.report_path.parent,
            settings.external_cache_path.parent,
        ]
    )
    dir_status: dict[str, str] = {}
    for directory in writable_dirs:
        try:
            _assert_writable(directory)
            dir_status[str(directory)] = "ok"
        except OSError as exc:
            errors.append(f"directory_not_writable:{directory}")
            dir_status[str(directory)] = f"error:{exc}"

    disk_target = settings.db_path.parent if settings.db_path.parent.exists() else Path(".")
    disk_details: dict[str, Any] = {"path": str(disk_target.resolve())}
    try:
        usage = shutil.disk_usage(disk_target)
        disk_details.update({"free_bytes": usage.free, "total_bytes": usage.total})
        if settings.minimum_free_disk_bytes > 0 and usage.free < settings.minimum_free_disk_bytes:
            code = "disk_headroom_below_threshold"
            if settings.enforce_disk_headroom:
                errors.append(code)
            else:
                warnings.append(code)
    except OSError as exc:
        warnings.append("disk_headroom_unavailable")
        disk_details["error"] = str(exc)

    doctrine = doctrine_summary()
    details = {
        "doctrine": {
            "master_thesis": doctrine["master_thesis"],
            "operating_hierarchy": doctrine["operating_hierarchy"],
            "gated_modules": doctrine["gated_modules"],
            "benchmark_question": "What can this machine do that a simple SPY/QQQ allocator cannot?",
        },
        "writable_directories": dir_status,
        "disk": disk_details,
        "heartbeat_path": str(settings.heartbeat_path),
        "runtime_status_path": str(settings.runtime_status_path),
        "kalshi": {
            "api_url": settings.kalshi_api_url,
            "mode": settings.kalshi_mode,
            "data_mode": settings.kalshi_data_mode,
            "auth_configured": bool(settings.kalshi_api_key_id and settings.kalshi_private_key_path),
            "private_key_path": settings.kalshi_private_key_path,
            "private_key_exists": bool(settings.kalshi_private_key_path and Path(settings.kalshi_private_key_path).exists()),
            "request_timeout_seconds": settings.kalshi_request_timeout_seconds,
            "request_retries": settings.kalshi_request_retries,
            "connectivity_report_path": str(settings.kalshi_connectivity_report_path),
        },
        "trend": {
            "data_mode": settings.trend_data_mode,
            "weight_mode": settings.trend_weight_mode,
            "symbols": settings.trend_symbols,
            "fetch_timeout_seconds": settings.trend_fetch_timeout_seconds,
            "fetch_retries": settings.trend_fetch_retries,
            "first_measurement_report_path": str(settings.first_measurement_report_path),
            "strategy_transparency_pack_path": str(settings.strategy_transparency_pack_path),
        },
    }
    return StartupCheckResult(
        runtime_name=runtime_name,
        checked_at_utc=checked_at,
        errors=errors,
        warnings=sorted(set(warnings)),
        details=details,
    )


def assert_runtime_headroom(settings: Settings) -> dict[str, Any]:
    disk_target = settings.db_path.parent if settings.db_path.parent.exists() else Path(".")
    usage = shutil.disk_usage(disk_target)
    details = {
        "path": str(disk_target.resolve()),
        "free_bytes": usage.free,
        "total_bytes": usage.total,
        "minimum_required_bytes": settings.minimum_free_disk_bytes,
    }
    if settings.minimum_free_disk_bytes > 0 and usage.free < settings.minimum_free_disk_bytes and settings.enforce_disk_headroom:
        raise RuntimeError(json.dumps({"error": "disk_headroom_below_threshold", **details}, sort_keys=True))
    return details


def write_runtime_status(
    settings: Settings,
    runtime_name: str,
    state: str,
    *,
    message: str | None = None,
    details: dict[str, Any] | None = None,
    startup_check: StartupCheckResult | None = None,
    update_heartbeat: bool = False,
    now: datetime | None = None,
) -> dict[str, Any]:
    now = now or datetime.now(timezone.utc)
    previous = _read_status(settings.runtime_status_path)
    payload: dict[str, Any] = {
        "schema_version": STATUS_SCHEMA_VERSION,
        "runtime_name": runtime_name,
        "state": state,
        "message": message or "",
        "updated_at_utc": now.isoformat(),
        "hostname": socket.gethostname(),
        "pid": os.getpid(),
        "heartbeat_path": str(settings.heartbeat_path),
        "report_path": str(settings.report_path),
        "db_path": str(settings.db_path),
        "details": details or {},
        "last_successful_cycle_utc": previous.get("last_successful_cycle_utc"),
        "last_error_utc": previous.get("last_error_utc"),
        "last_error_message": previous.get("last_error_message", ""),
    }
    if startup_check is not None:
        payload["startup_check"] = {
            "runtime_name": startup_check.runtime_name,
            "checked_at_utc": startup_check.checked_at_utc,
            "errors": startup_check.errors,
            "warnings": startup_check.warnings,
            "details": startup_check.details,
        }
    elif "startup_check" in previous:
        payload["startup_check"] = previous["startup_check"]
    if state == "running":
        payload["last_successful_cycle_utc"] = now.isoformat()
    if state in {"error", "halted"}:
        payload["last_error_utc"] = now.isoformat()
        payload["last_error_message"] = message or ""
    settings.runtime_status_path.write_text(json.dumps(payload, sort_keys=True, indent=2), encoding="utf-8")
    if update_heartbeat:
        settings.heartbeat_path.write_text(str(int(now.timestamp())), encoding="utf-8")
    return payload


def _read_status(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def _assert_writable(directory: Path) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=directory, prefix=".runtime_check_", delete=True):
        pass


def _unique_dirs(paths: list[Path]) -> list[Path]:
    seen: set[str] = set()
    items: list[Path] = []
    for path in paths:
        resolved = str(path.resolve())
        if resolved in seen:
            continue
        seen.add(resolved)
        items.append(path)
    return items
