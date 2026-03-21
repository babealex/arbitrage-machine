from app.market_data.orderbook import estimate_fee_cents, snapshot_from_orderbook_payload


def test_estimate_fee_is_positive() -> None:
    assert estimate_fee_cents(45, 10) > 0


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
