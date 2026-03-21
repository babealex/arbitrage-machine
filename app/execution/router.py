from __future__ import annotations

import logging

from app.execution.orders import build_order_intent, reverse_leg, to_api_payload
from app.kalshi.rest_client import KalshiRestClient
from app.models import Fill, Signal
from app.persistence.contracts import KalshiOrderRecord
from app.persistence.kalshi_repo import KalshiRepository
from app.portfolio.state import PortfolioState
from app.risk.manager import RiskManager


class ExecutionRouter:
    def __init__(self, client: KalshiRestClient, repo: KalshiRepository, portfolio: PortfolioState, risk: RiskManager, logger: logging.Logger, live_trading: bool) -> None:
        self.client = client
        self.repo = repo
        self.portfolio = portfolio
        self.risk = risk
        self.logger = logger
        self.live_trading = live_trading

    def execute_signal(self, signal: Signal) -> bool:
        if not self.risk.approve_signal(signal, self.portfolio.snapshot):
            self.logger.info("signal_rejected_by_risk", extra={"event": {"ticker": signal.ticker, "strategy": signal.strategy.value, "edge": signal.probability_edge}})
            return False
        self.repo.persist_signal(signal)
        completed: list[dict] = []
        self.logger.info("order_batch_started", extra={"event": {"ticker": signal.ticker, "strategy": signal.strategy.value, "legs": signal.legs, "quantity": signal.quantity}})
        for index, leg in enumerate(signal.legs):
            seed = f"{signal.strategy.value}:{signal.ticker}:{index}:{signal.quantity}:{leg['ticker']}:{leg['side']}:{leg['price']}"
            intent = build_order_intent(
                ticker=leg["ticker"],
                side=leg["side"],
                action=leg["action"],
                quantity=signal.quantity,
                price=int(leg["price"]),
                tif=leg["tif"],
                seed=seed,
            )
            self.repo.persist_order(KalshiOrderRecord(intent=intent, status="created", metadata={"signal": signal.strategy.value}))
            self.logger.info("order_attempt", extra={"event": {"ticker": intent.ticker, "side": intent.side.value, "action": intent.action, "price": intent.price, "quantity": intent.quantity, "tif": intent.time_in_force}})
            if not self.live_trading:
                self.repo.persist_order(KalshiOrderRecord(intent=intent, status="simulated"))
                self.repo.persist_fill(Fill(order_id=intent.client_order_id, ticker=intent.ticker, quantity=intent.quantity, price=intent.price, side=intent.side))
                completed.append(leg)
                self.logger.info("simulated_order", extra={"event": {"client_order_id": intent.client_order_id, "ticker": intent.ticker, "side": intent.side.value, "action": intent.action, "price": intent.price, "quantity": intent.quantity, "edge": signal.probability_edge}})
                self.logger.info("simulated_fill", extra={"event": {"client_order_id": intent.client_order_id, "ticker": intent.ticker, "side": intent.side.value, "price": intent.price, "quantity": intent.quantity}})
                continue
            try:
                response = self.client.create_order(to_api_payload(intent))
                self.repo.persist_order(KalshiOrderRecord(intent=intent, status="accepted", metadata=response))
                order_id = str(response.get("order", {}).get("order_id", response.get("order_id", intent.client_order_id)))
                final_status = self.client.get_order(order_id) if order_id else response
                self.repo.persist_order(KalshiOrderRecord(intent=intent, status=str(final_status.get("status", "accepted")), metadata=final_status))
                self.repo.persist_fill(Fill(order_id=intent.client_order_id, ticker=intent.ticker, quantity=intent.quantity, price=intent.price, side=intent.side))
                completed.append(leg)
                self.portfolio.reconcile(self.repo)
                self.logger.info("order_outcome", extra={"event": {"client_order_id": intent.client_order_id, "ticker": intent.ticker, "response": final_status}})
            except Exception as exc:
                self.risk.record_failed_trade()
                self.repo.persist_order(KalshiOrderRecord(intent=intent, status="rejected", metadata={"error": str(exc)}))
                self.logger.error("order_failed", extra={"event": {"ticker": intent.ticker, "error": str(exc)}})
                self._flatten(completed, signal)
                return False
        return True

    def _flatten(self, completed: list[dict], signal: Signal) -> None:
        for leg in reversed(completed):
            reverse = reverse_leg(leg)
            for attempt in range(2):
                seed = f"flatten:{signal.ticker}:{leg['ticker']}:{attempt}"
                intent = build_order_intent(reverse["ticker"], reverse["side"], reverse["action"], signal.quantity, reverse["price"], reverse["tif"], seed)
                self.repo.persist_order(KalshiOrderRecord(intent=intent, status="flatten_created", metadata={"original_signal": signal.strategy.value}))
                self.logger.warning("flatten_attempt", extra={"event": {"ticker": intent.ticker, "side": intent.side.value, "action": intent.action, "attempt": attempt + 1}})
                if not self.live_trading:
                    self.repo.persist_order(KalshiOrderRecord(intent=intent, status="flatten_simulated"))
                    break
                try:
                    response = self.client.create_order(to_api_payload(intent))
                    self.repo.persist_order(KalshiOrderRecord(intent=intent, status="flatten_accepted", metadata=response))
                    self.logger.warning("flatten_success", extra={"event": {"ticker": intent.ticker, "response": response}})
                    self.portfolio.reconcile(self.repo)
                    break
                except Exception as exc:
                    self.repo.persist_order(KalshiOrderRecord(intent=intent, status="flatten_rejected", metadata={"error": str(exc)}))
                    self.logger.error("flatten_failed", extra={"event": {"ticker": intent.ticker, "error": str(exc), "attempt": attempt + 1}})
