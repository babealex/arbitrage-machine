from datetime import datetime, timezone

from app.external_data.probability_mapper import map_market_probability
from app.models import ExternalDistribution, Market


def test_probability_mapper_handles_above_contract_with_explicit_definition() -> None:
    market = Market(
        ticker="FED-JUN26-ABOVE-5.25",
        event_ticker="FED",
        title="Fed target rate above 5.25 after June 2026 meeting",
        subtitle="",
        status="open",
        metadata={
            "contract_definition": {
                "family": "fed_target_rate",
                "meeting_key": "2026-06",
                "settlement": "upper_bound",
                "lower_bound": 5.25,
                "upper_bound": None,
                "lower_inclusive": False,
                "upper_inclusive": False,
            }
        },
    )
    distributions = {
        "2026-06": ExternalDistribution(source="fed_futures", key="2026-06", values={5.25: 0.3, 5.50: 0.5, 5.75: 0.2}, as_of=datetime.now(timezone.utc), metadata={"settlement": "upper_bound"})
    }
    probability, metadata = map_market_probability(market, distributions)
    assert probability == 0.7
    assert metadata["reason"] == "mapped"


def test_probability_mapper_handles_between_bin_and_midpoint_conversion() -> None:
    market = Market(
        ticker="FED-JUN26-BETWEEN",
        event_ticker="FED",
        title="Fed target rate between 5.125 and 5.375 after June 2026 meeting midpoint",
        subtitle="",
        status="open",
        metadata={
            "contract_definition": {
                "family": "fed_target_rate",
                "meeting_key": "2026-06",
                    "settlement": "midpoint",
                    "lower_bound": 5.125,
                    "upper_bound": 5.375,
                    "lower_inclusive": True,
                    "upper_inclusive": False,
                }
            },
        )
    distributions = {
        "2026-06": ExternalDistribution(source="fed_futures", key="2026-06", values={5.25: 0.3, 5.50: 0.7}, as_of=datetime.now(timezone.utc), metadata={"settlement": "upper_bound"})
    }
    probability, _ = map_market_probability(market, distributions)
    assert probability == 0.3
