from app.sleeves import SLEEVE_REGISTRY
from app.strategy_doctrine import MASTER_THESIS, SLEEVE_DOCTRINE, doctrine_summary


def test_doctrine_summary_matches_intended_operating_hierarchy() -> None:
    summary = doctrine_summary()

    assert "capital-allocation machine" in summary["master_thesis"]
    assert summary["operating_hierarchy"] == ["trend", "kalshi_event"]
    assert summary["gated_modules"] == ["options", "convexity"]


def test_sleeve_registry_descriptions_are_sourced_from_doctrine() -> None:
    assert SLEEVE_REGISTRY["trend"].description == SLEEVE_DOCTRINE["trend"].mandate
    assert SLEEVE_REGISTRY["kalshi_event"].description == SLEEVE_DOCTRINE["kalshi_event"].mandate
    assert SLEEVE_REGISTRY["options"].description == SLEEVE_DOCTRINE["options"].mandate
    assert SLEEVE_REGISTRY["convexity"].description == SLEEVE_DOCTRINE["convexity"].mandate


def test_doctrine_is_trend_first_and_options_gated() -> None:
    assert "trend" in MASTER_THESIS
    assert SLEEVE_DOCTRINE["trend"].role == "core_engine"
    assert SLEEVE_DOCTRINE["kalshi_event"].role == "booster_sleeve"
    assert SLEEVE_DOCTRINE["options"].role == "gated_experimental"
    assert SLEEVE_DOCTRINE["convexity"].role == "gated_experimental"
