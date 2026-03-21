from __future__ import annotations

from app.models import OrderBookLevel, OrderBookSnapshot


def build_levels(levels: list[list[int]] | list[dict]) -> list[OrderBookLevel]:
    built: list[OrderBookLevel] = []
    for level in levels or []:
        if isinstance(level, dict):
            price = float(level["price"])
            size = float(level["quantity"])
        else:
            price = float(level[0])
            size = float(level[1])
        if price > 1:
            price = price / 100.0
        built.append(OrderBookLevel(price=price, size=int(size)))
    return built


def derive_reciprocal_levels(yes_levels: list[OrderBookLevel]) -> list[OrderBookLevel]:
    reciprocal = [OrderBookLevel(price=round(1.0 - level.price, 4), size=level.size) for level in yes_levels]
    reciprocal.sort(key=lambda item: item.price, reverse=True)
    return reciprocal


def snapshot_from_orderbook_payload(ticker: str, payload: dict) -> OrderBookSnapshot:
    if "orderbook_fp" in payload:
        fp = payload.get("orderbook_fp", {})
        yes_bids = build_levels(fp.get("yes_dollars", []))
        no_bids = build_levels(fp.get("no_dollars", []))
        yes_asks = derive_reciprocal_levels(no_bids)
        no_asks = derive_reciprocal_levels(yes_bids)
    else:
        yes_bids = build_levels(payload.get("yes_bids", []))
        yes_asks = build_levels(payload.get("yes_asks", []))
        no_bids = build_levels(payload.get("no_bids", [])) or derive_reciprocal_levels(yes_asks)
        no_asks = build_levels(payload.get("no_asks", [])) or derive_reciprocal_levels(yes_bids)
    yes_bids.sort(key=lambda item: item.price, reverse=True)
    yes_asks.sort(key=lambda item: item.price)
    no_bids.sort(key=lambda item: item.price, reverse=True)
    no_asks.sort(key=lambda item: item.price)
    return OrderBookSnapshot(
        market_ticker=ticker,
        yes_bids=yes_bids,
        yes_asks=yes_asks,
        no_bids=no_bids,
        no_asks=no_asks,
    )


def estimate_fee_cents(price_cents: int, contracts: int) -> int:
    return max(1, int(round(0.035 * price_cents * contracts / 100.0 + 0.07 * contracts)))


def fee_per_contract(price_cents: int, contracts: int = 1) -> float:
    return estimate_fee_cents(price_cents, contracts) / max(contracts, 1) / 100.0


def best_quotes_in_cents(snapshot: OrderBookSnapshot | None) -> dict[str, int | None]:
    if snapshot is None:
        return {
            "best_yes_bid": None,
            "best_yes_ask": None,
            "best_no_bid": None,
            "best_no_ask": None,
            "best_yes_bid_size": 0,
            "best_yes_ask_size": 0,
            "best_no_bid_size": 0,
            "best_no_ask_size": 0,
        }
    return {
        "best_yes_bid": _price_to_cents(snapshot.best_yes_bid()),
        "best_yes_ask": _price_to_cents(snapshot.best_yes_ask()),
        "best_no_bid": _price_to_cents(snapshot.best_no_bid()),
        "best_no_ask": _price_to_cents(snapshot.best_no_ask()),
        "best_yes_bid_size": snapshot.best_yes_bid_size(),
        "best_yes_ask_size": snapshot.best_yes_ask_size(),
        "best_no_bid_size": snapshot.best_no_bid_size(),
        "best_no_ask_size": snapshot.best_no_ask_size(),
    }


def _price_to_cents(price: float | None) -> int | None:
    if price is None:
        return None
    return int(round(price * 100))
