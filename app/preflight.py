from __future__ import annotations

import json
import sys

from app.common.bootstrap import load_runtime_settings
from app.common.runtime_health import run_startup_checks, write_runtime_status


def _runtime_name_for_settings(settings) -> str:
    if settings.enable_event_driven_mode and settings.event_trading_only_mode:
        return "event_driven"
    if settings.enable_belief_layer and settings.belief_layer_only_mode:
        return "belief_layer"
    return "layer2_kalshi"


def main() -> int:
    settings = load_runtime_settings()
    runtime_name = _runtime_name_for_settings(settings)
    result = run_startup_checks(settings, runtime_name)
    write_runtime_status(
        settings,
        runtime_name,
        "preflight_ok" if result.ok else "preflight_failed",
        message="" if result.ok else ",".join(result.errors),
        details={"warnings": result.warnings, "errors": result.errors},
        startup_check=result,
    )
    payload = {
        "runtime_name": result.runtime_name,
        "checked_at_utc": result.checked_at_utc,
        "ok": result.ok,
        "errors": result.errors,
        "warnings": result.warnings,
        "details": result.details,
    }
    sys.stdout.write(json.dumps(payload, sort_keys=True, indent=2))
    sys.stdout.write("\n")
    return 0 if result.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
