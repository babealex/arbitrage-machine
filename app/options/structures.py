from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal

from app.options.models import OptionContract


@dataclass(frozen=True, slots=True)
class OptionStructureLeg:
    contract: OptionContract
    quantity: int


@dataclass(frozen=True, slots=True)
class OptionStructure:
    underlying: str
    name: str
    legs: list[OptionStructureLeg] = field(default_factory=list)


def call_vertical_spread(
    *,
    underlying: str,
    expiration_date: date,
    long_strike: Decimal,
    short_strike: Decimal,
    quantity: int = 1,
    name: str | None = None,
) -> OptionStructure:
    return OptionStructure(
        underlying=underlying,
        name=name or f"call_vertical_{long_strike}_{short_strike}",
        legs=[
            OptionStructureLeg(
                contract=OptionContract(
                    underlying=underlying,
                    expiration_date=expiration_date,
                    strike=long_strike,
                    option_type="call",
                ),
                quantity=quantity,
            ),
            OptionStructureLeg(
                contract=OptionContract(
                    underlying=underlying,
                    expiration_date=expiration_date,
                    strike=short_strike,
                    option_type="call",
                ),
                quantity=-quantity,
            ),
        ],
    )


def put_vertical_spread(
    *,
    underlying: str,
    expiration_date: date,
    long_strike: Decimal,
    short_strike: Decimal,
    quantity: int = 1,
    name: str | None = None,
) -> OptionStructure:
    return OptionStructure(
        underlying=underlying,
        name=name or f"put_vertical_{long_strike}_{short_strike}",
        legs=[
            OptionStructureLeg(
                contract=OptionContract(
                    underlying=underlying,
                    expiration_date=expiration_date,
                    strike=long_strike,
                    option_type="put",
                ),
                quantity=quantity,
            ),
            OptionStructureLeg(
                contract=OptionContract(
                    underlying=underlying,
                    expiration_date=expiration_date,
                    strike=short_strike,
                    option_type="put",
                ),
                quantity=-quantity,
            ),
        ],
    )


def long_straddle(
    *,
    underlying: str,
    expiration_date: date,
    strike: Decimal,
    quantity: int = 1,
    name: str | None = None,
) -> OptionStructure:
    return OptionStructure(
        underlying=underlying,
        name=name or f"long_straddle_{strike}",
        legs=[
            OptionStructureLeg(
                contract=OptionContract(
                    underlying=underlying,
                    expiration_date=expiration_date,
                    strike=strike,
                    option_type="call",
                ),
                quantity=quantity,
            ),
            OptionStructureLeg(
                contract=OptionContract(
                    underlying=underlying,
                    expiration_date=expiration_date,
                    strike=strike,
                    option_type="put",
                ),
                quantity=quantity,
            ),
        ],
    )


def call_calendar(
    *,
    underlying: str,
    near_expiration_date: date,
    far_expiration_date: date,
    strike: Decimal,
    quantity: int = 1,
    name: str | None = None,
) -> OptionStructure:
    return OptionStructure(
        underlying=underlying,
        name=name or f"call_calendar_{strike}",
        legs=[
            OptionStructureLeg(
                contract=OptionContract(
                    underlying=underlying,
                    expiration_date=near_expiration_date,
                    strike=strike,
                    option_type="call",
                ),
                quantity=-quantity,
            ),
            OptionStructureLeg(
                contract=OptionContract(
                    underlying=underlying,
                    expiration_date=far_expiration_date,
                    strike=strike,
                    option_type="call",
                ),
                quantity=quantity,
            ),
        ],
    )
