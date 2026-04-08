from __future__ import annotations

from decimal import Decimal

from app.options.models import ChainSurfaceFeatures, ExpirySurfaceFeatures, OptionChainSnapshot, OptionStateVector
from app.options.surface_features import build_surface_features


def build_state_vector(snapshot: OptionChainSnapshot) -> tuple[list[ExpirySurfaceFeatures], ChainSurfaceFeatures, OptionStateVector]:
    expiry_features, chain_features = build_surface_features(snapshot)
    forward_price = snapshot.forward_price if snapshot.forward_price is not None else snapshot.spot_price
    total_variance_term_monotonic = _total_variance_term_monotonic(expiry_features)
    near = expiry_features[0] if expiry_features else None
    vrp_proxy_near = None
    if near is not None and near.atm_iv is not None and snapshot.realized_vol_20d is not None:
        vrp_proxy_near = (near.atm_iv * near.atm_iv) - (snapshot.realized_vol_20d * snapshot.realized_vol_20d)
    return expiry_features, chain_features, OptionStateVector(
        underlying=snapshot.underlying,
        observed_at=snapshot.observed_at,
        source=snapshot.source,
        spot_price=snapshot.spot_price,
        forward_price=forward_price,
        realized_vol_20d=snapshot.realized_vol_20d,
        atm_iv_near=chain_features.atm_iv_near,
        atm_iv_next=chain_features.atm_iv_next,
        atm_iv_term_slope=chain_features.atm_iv_term_slope,
        event_premium_proxy=chain_features.event_premium_proxy,
        vrp_proxy_near=vrp_proxy_near,
        total_variance_term_monotonic=total_variance_term_monotonic,
    )


def _total_variance_term_monotonic(expiry_features: list[ExpirySurfaceFeatures]) -> bool:
    last_value: Decimal | None = None
    for item in expiry_features:
        if item.atm_total_variance is None:
            continue
        if last_value is not None and item.atm_total_variance < last_value:
            return False
        last_value = item.atm_total_variance
    return True
