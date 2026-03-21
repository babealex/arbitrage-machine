from app.config import Settings
from app.market_data.filters import filter_structural_markets
from app.models import Market


def test_filter_structural_markets_prefers_macro_and_excludes_sports() -> None:
    settings = Settings()
    markets = [
        Market(ticker="FED-2026", event_ticker="FED", title="Fed target rate above 5.25", subtitle="", status="open"),
        Market(ticker="NBA-2026", event_ticker="NBA", title="Championship winner", subtitle="", status="open"),
    ]
    result = filter_structural_markets(markets, settings)
    assert [market.ticker for market in result.included] == ["FED-2026"]
    assert result.excluded[0][1] == "excluded_keyword"
    assert result.excluded[0][2] in {"title", "ticker", "event_ticker"}
    assert result.diagnostics[0]["included"] is True
    assert result.diagnostics[1]["matched_rule_type"] == "excluded"
