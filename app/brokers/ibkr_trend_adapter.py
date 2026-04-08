from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Protocol

from app.brokers.base import TrendBrokerAdapter
from app.brokers.models import (
    AccountSnapshot,
    BrokerFill,
    BrokerOrder,
    BrokerOrderAck,
    BrokerReadinessResult,
    BrokerPosition,
    ReconciliationResult,
    TrendOrderRequest,
)
from app.execution.equity_broker import EquityExecutionBroker
from app.execution.models import ExecutionOutcome, OrderIntent
from app.market_data.snapshots import MarketStateInput
from app.persistence.kalshi_repo import KalshiRepository
from app.persistence.trend_broker_repo import TrendBrokerRepository
from app.persistence.trend_repo import TrendRepository


class IbkrClientProtocol(Protocol):
    def connect(self, host: str, port: int, client_id: int, readonly: bool = False) -> None: ...
    def disconnect(self) -> None: ...
    def is_connected(self) -> bool: ...
    def get_account_summary(self, account_id: str | None = None) -> dict[str, Any]: ...
    def get_positions(self, account_id: str | None = None) -> list[dict[str, Any]]: ...
    def get_open_orders(self, account_id: str | None = None) -> list[dict[str, Any]]: ...
    def place_order(self, order: TrendOrderRequest, account_id: str | None = None) -> dict[str, Any]: ...
    def cancel_order(self, broker_order_id: str) -> None: ...
    def get_fills(self, since: datetime | None = None, account_id: str | None = None) -> list[dict[str, Any]]: ...


class MissingIbkrClient:
    def connect(self, host: str, port: int, client_id: int, readonly: bool = False) -> None:
        raise RuntimeError("ibkr_dependency_missing")

    def disconnect(self) -> None:
        return None

    def is_connected(self) -> bool:
        return False

    def get_account_summary(self, account_id: str | None = None) -> dict[str, Any]:
        raise RuntimeError("ibkr_dependency_missing")

    def get_positions(self, account_id: str | None = None) -> list[dict[str, Any]]:
        raise RuntimeError("ibkr_dependency_missing")

    def get_open_orders(self, account_id: str | None = None) -> list[dict[str, Any]]:
        raise RuntimeError("ibkr_dependency_missing")

    def place_order(self, order: TrendOrderRequest, account_id: str | None = None) -> dict[str, Any]:
        raise RuntimeError("ibkr_dependency_missing")

    def cancel_order(self, broker_order_id: str) -> None:
        raise RuntimeError("ibkr_dependency_missing")

    def get_fills(self, since: datetime | None = None, account_id: str | None = None) -> list[dict[str, Any]]:
        raise RuntimeError("ibkr_dependency_missing")


class IbInsyncClient:
    def __init__(self) -> None:
        from ib_insync import IB  # type: ignore

        self._ib = IB()

    def connect(self, host: str, port: int, client_id: int, readonly: bool = False) -> None:
        self._ib.connect(host, port, clientId=client_id, readonly=readonly)

    def disconnect(self) -> None:
        if self._ib.isConnected():
            self._ib.disconnect()

    def is_connected(self) -> bool:
        return bool(self._ib.isConnected())

    def get_account_summary(self, account_id: str | None = None) -> dict[str, Any]:
        summary = self._ib.accountSummary(account=account_id or "")
        values = {item.tag: item.value for item in summary}
        return {
            "account_id": account_id or values.get("AccountCode", ""),
            "net_liquidation": values.get("NetLiquidation", "0"),
            "available_funds": values.get("AvailableFunds", "0"),
            "buying_power": values.get("BuyingPower", "0"),
            "currency": values.get("Currency", "USD"),
            "timestamp": datetime.now(timezone.utc),
        }

    def get_positions(self, account_id: str | None = None) -> list[dict[str, Any]]:
        items = []
        for position in self._ib.positions(account=account_id or ""):
            items.append(
                {
                    "symbol": position.contract.symbol,
                    "quantity": str(position.position),
                    "avg_cost": str(position.avgCost),
                    "market_price": None,
                    "source_timestamp": datetime.now(timezone.utc),
                }
            )
        return items

    def get_open_orders(self, account_id: str | None = None) -> list[dict[str, Any]]:
        items = []
        for trade in self._ib.openTrades():
            order = trade.order
            contract = trade.contract
            items.append(
                {
                    "broker_order_id": str(order.orderId),
                    "client_order_id": str(order.orderRef or order.orderId),
                    "symbol": contract.symbol,
                    "side": "buy" if order.action.upper() == "BUY" else "sell",
                    "quantity": str(order.totalQuantity),
                    "order_type": str(order.orderType).lower(),
                    "limit_price": None if float(getattr(order, "lmtPrice", 0) or 0) <= 0 else str(order.lmtPrice),
                    "time_in_force": str(order.tif),
                    "status": str(trade.orderStatus.status).lower(),
                    "created_at": datetime.now(timezone.utc),
                }
            )
        return items

    def place_order(self, order: TrendOrderRequest, account_id: str | None = None) -> dict[str, Any]:
        from ib_insync import Order, Stock  # type: ignore

        contract = Stock(order.symbol, "SMART", "USD")
        ib_order = Order(
            action="BUY" if order.side.lower() == "buy" else "SELL",
            totalQuantity=float(order.quantity),
            orderType=order.order_type.upper(),
            tif=order.time_in_force,
            orderRef=order.client_order_id,
        )
        if order.limit_price is not None:
            ib_order.lmtPrice = float(order.limit_price)
        trade = self._ib.placeOrder(contract, ib_order)
        return {
            "broker_order_id": str(trade.order.orderId),
            "client_order_id": order.client_order_id,
            "status": str(trade.orderStatus.status).lower(),
            "received_at": datetime.now(timezone.utc),
            "reject_reason": None,
        }

    def cancel_order(self, broker_order_id: str) -> None:
        order_id = int(broker_order_id)
        for trade in self._ib.openTrades():
            if int(trade.order.orderId) == order_id:
                self._ib.cancelOrder(trade.order)
                return
        raise RuntimeError(f"ibkr_order_not_found:{broker_order_id}")

    def get_fills(self, since: datetime | None = None, account_id: str | None = None) -> list[dict[str, Any]]:
        items = []
        for fill in self._ib.fills():
            fill_time = fill.time.replace(tzinfo=timezone.utc)
            if since is not None and fill_time < since:
                continue
            items.append(
                {
                    "broker_fill_id": str(fill.execution.execId),
                    "broker_order_id": str(fill.execution.orderId),
                    "client_order_id": str(fill.execution.orderRef or fill.execution.orderId),
                    "symbol": fill.contract.symbol,
                    "quantity": str(fill.execution.shares),
                    "price": str(fill.execution.price),
                    "side": "buy" if fill.execution.side.upper() == "BOT" else "sell",
                    "filled_at": fill_time,
                }
            )
        return items


def default_ibkr_client_factory() -> IbkrClientProtocol:
    try:
        return IbInsyncClient()
    except Exception:
        return MissingIbkrClient()


@dataclass(slots=True)
class IbkrTrendBrokerAdapter(TrendBrokerAdapter, EquityExecutionBroker):
    config: Any
    logger: Any
    trend_repo: TrendRepository
    signal_repo: KalshiRepository
    broker_repo: TrendBrokerRepository
    client: IbkrClientProtocol | None = None

    def __post_init__(self) -> None:
        self.broker_name = "ibkr"
        self.client = self.client or default_ibkr_client_factory()
        self._last_successful_auth_at: datetime | None = None
        self._last_market_data_check_at: datetime | None = None
        self._last_reconciliation_check_at: datetime | None = None
        self._reconnect_failures = 0
        self._next_reconnect_not_before: datetime | None = None

    def is_live_capable(self) -> bool:
        return self.readiness_check().is_ready

    def readiness_check(self) -> BrokerReadinessResult:
        checked_at = datetime.now(timezone.utc)
        reasons: list[str] = []
        if getattr(self.config, "trend_live_broker_mode", "none") != "ibkr":
            reasons.append("trend_broker_mode_not_ibkr")
        if not getattr(self.config, "trend_live_broker_enabled", False):
            reasons.append("trend_live_broker_disabled")
        reasons.extend(self._missing_config_fields())
        if not reasons:
            if not self._connect_if_needed():
                reasons.append("ibkr_connect_failed")
            else:
                try:
                    self.get_account_snapshot()
                    self.get_positions()
                    self.get_open_orders()
                    reconciliation = self.reconcile()
                    if not reconciliation.is_consistent:
                        reasons.extend(reconciliation.reasons or ["reconciliation_failed"])
                except Exception as exc:
                    reasons.append(str(exc))
        result = BrokerReadinessResult(
            is_ready=not reasons,
            reasons=reasons,
            broker_name=self.broker_name,
            checked_at=checked_at,
            last_successful_auth_at=self._last_successful_auth_at,
            last_market_data_check_at=self._last_market_data_check_at,
            last_reconciliation_check_at=self._last_reconciliation_check_at,
        )
        self.broker_repo.persist_readiness_check(result)
        return result

    def get_account_snapshot(self) -> AccountSnapshot:
        raw = self.client.get_account_summary(getattr(self.config, "ibkr_account_id", "") or None)
        snapshot = AccountSnapshot(
            account_id=str(raw["account_id"]),
            net_liquidation=Decimal(str(raw["net_liquidation"])),
            available_funds=Decimal(str(raw["available_funds"])),
            buying_power=Decimal(str(raw["buying_power"])),
            currency=str(raw.get("currency", "USD")),
            timestamp=raw.get("timestamp", datetime.now(timezone.utc)),
        )
        self._last_market_data_check_at = snapshot.timestamp
        return snapshot

    def get_positions(self) -> list[BrokerPosition]:
        return [
            BrokerPosition(
                symbol=str(item["symbol"]),
                quantity=Decimal(str(item["quantity"])),
                avg_cost=Decimal(str(item["avg_cost"])),
                market_price=None if item.get("market_price") is None else Decimal(str(item["market_price"])),
                source_timestamp=item.get("source_timestamp", datetime.now(timezone.utc)),
            )
            for item in self.client.get_positions(getattr(self.config, "ibkr_account_id", "") or None)
        ]

    def get_open_orders(self) -> list[BrokerOrder]:
        return [
            BrokerOrder(
                broker_order_id=str(item["broker_order_id"]),
                client_order_id=str(item["client_order_id"]),
                symbol=str(item["symbol"]),
                side=str(item["side"]),
                quantity=Decimal(str(item["quantity"])),
                order_type=str(item["order_type"]),
                limit_price=None if item.get("limit_price") is None else Decimal(str(item["limit_price"])),
                time_in_force=str(item["time_in_force"]),
                status=str(item["status"]),
                created_at=item.get("created_at", datetime.now(timezone.utc)),
                reject_reason=item.get("reject_reason"),
            )
            for item in self.client.get_open_orders(getattr(self.config, "ibkr_account_id", "") or None)
        ]

    def place_order(self, order: TrendOrderRequest) -> BrokerOrderAck:
        ack_raw = self.client.place_order(order, getattr(self.config, "ibkr_account_id", "") or None)
        ack = BrokerOrderAck(
            broker_order_id=str(ack_raw["broker_order_id"]),
            client_order_id=str(ack_raw["client_order_id"]),
            status=str(ack_raw["status"]),
            received_at=ack_raw.get("received_at", datetime.now(timezone.utc)),
            reject_reason=ack_raw.get("reject_reason"),
        )
        self.broker_repo.persist_order_event(broker_name=self.broker_name, ack=ack)
        return ack

    def cancel_order(self, broker_order_id: str) -> None:
        self.client.cancel_order(broker_order_id)
        self.broker_repo.persist_session_event(
            broker_name=self.broker_name,
            event_type="cancel_order",
            status="ok",
            reason=broker_order_id,
            occurred_at=datetime.now(timezone.utc),
        )

    def get_fills(self, since: datetime | None = None) -> list[BrokerFill]:
        fills = [
            BrokerFill(
                broker_fill_id=str(item["broker_fill_id"]),
                broker_order_id=str(item["broker_order_id"]),
                client_order_id=str(item["client_order_id"]),
                symbol=str(item["symbol"]),
                quantity=Decimal(str(item["quantity"])),
                price=Decimal(str(item["price"])),
                side=str(item["side"]),
                filled_at=item.get("filled_at", datetime.now(timezone.utc)),
            )
            for item in self.client.get_fills(since, getattr(self.config, "ibkr_account_id", "") or None)
        ]
        for fill in fills:
            self.broker_repo.persist_fill(broker_name=self.broker_name, fill=fill)
        return fills

    def reconcile(self) -> ReconciliationResult:
        local_positions = {position.symbol: Decimal(position.quantity) for position in self.trend_repo.load_positions()}
        broker_positions = {position.symbol: position.quantity for position in self.get_positions()}
        position_diffs: list[str] = []
        for symbol in sorted(set(local_positions) | set(broker_positions)):
            local_qty = local_positions.get(symbol, Decimal("0"))
            broker_qty = broker_positions.get(symbol, Decimal("0"))
            if local_qty != broker_qty:
                position_diffs.append(f"{symbol}:{local_qty}->{broker_qty}")

        local_open_orders = {
            intent.order_id
            for intent in self.signal_repo.load_order_intents()
            if intent.instrument.venue == "live_equity"
        }
        broker_open_orders = {order.client_order_id for order in self.get_open_orders()}
        open_order_diffs = sorted(
            [f"local_only:{item}" for item in local_open_orders - broker_open_orders]
            + [f"broker_only:{item}" for item in broker_open_orders - local_open_orders]
        )

        cash_diff = None
        snapshots = self.trend_repo.load_equity_snapshots()
        if snapshots:
            local_cash = snapshots[-1].cash
            broker_cash = self.get_account_snapshot().available_funds
            cash_diff = broker_cash - local_cash
        reasons: list[str] = []
        if position_diffs:
            reasons.append("position_mismatch")
        if open_order_diffs:
            reasons.append("open_order_mismatch")
        if cash_diff is not None and abs(cash_diff) > Decimal("5"):
            reasons.append("cash_mismatch")
        result = ReconciliationResult(
            is_consistent=not reasons,
            position_diffs=position_diffs,
            open_order_diffs=open_order_diffs,
            cash_diff=cash_diff,
            reasons=reasons,
        )
        self._last_reconciliation_check_at = datetime.now(timezone.utc)
        self.broker_repo.persist_reconciliation_result(
            broker_name=self.broker_name,
            checked_at=self._last_reconciliation_check_at,
            result=result,
        )
        return result

    def execute_order(
        self,
        intent: OrderIntent,
        *,
        source_metadata: dict[str, object],
        market_state: MarketStateInput | None,
    ) -> ExecutionOutcome:
        readiness = self.readiness_check()
        if not readiness.is_ready:
            raise RuntimeError(",".join(readiness.reasons or ["ibkr_not_ready"]))
        request = TrendOrderRequest(
            symbol=intent.instrument.symbol,
            side=intent.side,
            quantity=intent.quantity_decimal,
            order_type=intent.order_type,
            limit_price=intent.limit_price,
            time_in_force=intent.time_in_force,
            client_order_id=intent.order_id,
        )
        ack = self.place_order(request)
        fills = self.get_fills()
        matched_fill = next((fill for fill in fills if fill.client_order_id == intent.order_id), None)
        status = ack.status
        filled_quantity = 0
        price_cents = None
        if matched_fill is not None:
            status = "filled"
            filled_quantity = int(matched_fill.quantity)
            price_cents = int((matched_fill.price * Decimal("100")).to_integral_value())
        elif ack.reject_reason:
            status = "rejected"
        reference_price_cents = None
        if market_state is not None:
            reference_price_cents = int((market_state.reference_price * Decimal("100")).to_integral_value())
        return ExecutionOutcome(
            client_order_id=intent.order_id,
            instrument=intent.instrument,
            side=intent.side,
            action=intent.order_type,
            status=status,
            requested_quantity=int(intent.quantity),
            filled_quantity=filled_quantity,
            price_cents=price_cents,
            time_in_force=intent.time_in_force,
            is_flatten=False,
            created_at=intent.created_at,
            metadata={
                **source_metadata,
                "broker_name": self.broker_name,
                "broker_order_id": ack.broker_order_id,
                "reject_reason": ack.reject_reason,
                "reference_price_cents": reference_price_cents,
            },
        )

    def _connect_if_needed(self) -> bool:
        now = datetime.now(timezone.utc)
        if self.client.is_connected():
            return True
        if self._next_reconnect_not_before is not None and now < self._next_reconnect_not_before:
            return False
        self.broker_repo.persist_session_event(
            broker_name=self.broker_name,
            event_type="reconnect",
            status="attempt",
            reason=None,
            occurred_at=now,
        )
        try:
            self.client.connect(
                getattr(self.config, "ibkr_host", "127.0.0.1"),
                int(getattr(self.config, "ibkr_port", 7497)),
                int(getattr(self.config, "ibkr_client_id", 0)),
                readonly=False,
            )
            self._last_successful_auth_at = now
            self._reconnect_failures = 0
            self._next_reconnect_not_before = None
            self.broker_repo.persist_session_event(
                broker_name=self.broker_name,
                event_type="reconnect",
                status="ok",
                reason=None,
                occurred_at=now,
            )
            return True
        except Exception as exc:
            self._reconnect_failures += 1
            delay_seconds = min(60, 2 ** min(self._reconnect_failures, 5))
            self._next_reconnect_not_before = now + timedelta(seconds=delay_seconds)
            self.broker_repo.persist_session_event(
                broker_name=self.broker_name,
                event_type="reconnect",
                status="failed",
                reason=str(exc),
                occurred_at=now,
            )
            self.logger.error("ibkr_reconnect_failed", extra={"event": {"error": str(exc), "delay_seconds": delay_seconds}})
            return False

    def _missing_config_fields(self) -> list[str]:
        missing = []
        if not getattr(self.config, "ibkr_host", ""):
            missing.append("ibkr_host_missing")
        if int(getattr(self.config, "ibkr_port", 0) or 0) <= 0:
            missing.append("ibkr_port_missing")
        if int(getattr(self.config, "ibkr_client_id", -1) or -1) < 0:
            missing.append("ibkr_client_id_missing")
        if not getattr(self.config, "ibkr_account_id", ""):
            missing.append("ibkr_account_id_missing")
        return missing


def evaluate_trend_live_readiness(adapter: TrendBrokerAdapter | None) -> BrokerReadinessResult:
    checked_at = datetime.now(timezone.utc)
    if adapter is None:
        return BrokerReadinessResult(
            is_ready=False,
            reasons=["trend_broker_adapter_missing"],
            broker_name="ibkr",
            checked_at=checked_at,
        )
    return adapter.readiness_check()
