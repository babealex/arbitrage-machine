from pathlib import Path

from app.events.paper_execution_validation import run_paper_execution_comparison


def test_paper_execution_comparison_shows_conservative_drag(tmp_path: Path) -> None:
    result = run_paper_execution_comparison(tmp_path / "comparison")
    assert result["comparison_schema_version"] == "event-paper-execution-comparison-v1"
    assert result["summary"]["legacy"]["trades_accepted"] >= result["summary"]["conservative"]["trades_accepted"]
    assert result["summary"]["legacy"]["fill_rate"] >= result["summary"]["conservative"]["fill_rate"]
    assert result["summary"]["conservative"]["partial_fill_frequency"] > 0.0
    assert result["summary"]["conservative"]["avg_realized_entry_disadvantage"] > 0.0
    passive = next(item for item in result["scenario_comparisons"] if item["scenario"] == "passive_non_fill")
    assert passive["legacy"]["trades_accepted"] == 1
    assert passive["conservative"]["trades_accepted"] == 0
