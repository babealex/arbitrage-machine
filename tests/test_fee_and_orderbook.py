from decimal import Decimal

from app.market_data.fees import balance_aligned_fee_dollars, edge_threshold, trade_fee_dollars
from app.market_data.orderbook import estimate_fee_cents, snapshot_from_orderbook_payload


def test_estimate_fee_is_positive() -> None:
    assert estimate_fee_cents(45, 10) > 0


def test_trade_fee_has_parabolic_shape() -> None:
    low = trade_fee_dollars(Decimal("0.10"), 1)
    middle = trade_fee_dollars(Decimal("0.50"), 1)
    high = trade_fee_dollars(Decimal("0.90"), 1)
    assert middle > low
    assert middle > high


def test_balance_aligned_fee_has_minimum_cent_cliff() -> None:
    assert balance_aligned_fee_dollars(Decimal("0.50"), 1, side="buy") >= Decimal("0.01")


def test_trade_fee_rounds_up_to_centicent() -> None:
    fee = trade_fee_dollars(Decimal("0.3301"), Decimal("0.03"))
    assert fee == Decimal("0.0005")


def test_balance_aligned_fee_supports_subpenny_and_fractional_intermediates() -> None:
    fee = balance_aligned_fee_dollars(Decimal("0.3301"), Decimal("0.03"), side="buy")
    assert fee == Decimal("0.010097")


def test_edge_threshold_uses_fee_and_buffer() -> None:
    gate = edge_threshold(edge_before_fees=Decimal("0.0500"), fee_estimate=Decimal("0.0100"), execution_buffer=Decimal("0.0050"))
    assert gate.edge_after_fees == Decimal("0.0350")
    assert gate.passes_bps(300)
    assert not gate.passes_bps(400)


def test_orderbook_derives_reciprocal_no_levels() -> None:
    snapshot = snapshot_from_orderbook_payload("MKT", {"yes_bids": [[40, 100]], "yes_asks": [[42, 100]]})
    assert snapshot.no_bids[0].price == 0.58
    assert snapshot.no_asks[0].price == 0.6


def test_orderbook_parses_orderbook_fp_payload() -> None:
    snapshot = snapshot_from_orderbook_payload(
        "MKT",
        {
            "orderbook_fp": {
                "yes_dollars": [["0.0100", "50.00"]],
                "no_dollars": [["0.9500", "85.00"]],
            }
        },
    )
    assert snapshot.best_yes_bid() == 0.01
    assert snapshot.best_no_bid() == 0.95
    assert snapshot.best_yes_ask() == 0.05
    assert snapshot.best_no_ask() == 0.99
