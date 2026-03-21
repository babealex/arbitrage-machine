import json
from pathlib import Path

from app.config import Settings
from app.db import Database
from app.execution.router import ExecutionRouter
from app.kalshi.rest_client import KalshiRestClient
from app.models import PortfolioSnapshot, Signal, StrategyEvaluation, StrategyName
from app.persistence.kalshi_repo import KalshiRepository
from app.persistence.reporting_repo import ReportingRepository
from app.portfolio.state import PortfolioState
from app.reporting import PaperTradingReporter
from app.risk.manager import RiskManager


class NeverCalledSession:
    def request(self, *args, **kwargs):
        raise RuntimeError("network unavailable")


def test_kalshi_client_data_only_mode_without_auth() -> None:
    client = KalshiRestClient(Settings(kalshi_api_key_id="", kalshi_private_key_path=""), session=NeverCalledSession())
    assert client.data_only_mode
    assert client.get_positions() == []
    assert client.get_balance() == {}
    assert client.list_orders() == []


class DummyPortfolio(PortfolioState):
    def __init__(self, client):
        super().__init__(client)
        self.snapshot = PortfolioSnapshot(equity=1000, cash=1000, daily_pnl=0, positions={})


def test_execution_router_simulates_without_live_submission(tmp_path: Path) -> None:
    client = KalshiRestClient(Settings(kalshi_api_key_id="", kalshi_private_key_path=""), session=NeverCalledSession())
    db = Database(tmp_path / "paper.db")
    repo = KalshiRepository(db)
    reporting_repo = ReportingRepository(db)
    router = ExecutionRouter(client, repo, DummyPortfolio(client), RiskManager(0.02, 0.30, 0.02, 2), __import__("logging").getLogger("test"), False)
    signal = Signal(
        strategy=StrategyName.CROSS_MARKET,
        ticker="FED-1",
        action="buy_yes",
        probability_edge=0.03,
        expected_edge_bps=300,
        quantity=2,
        legs=[{"ticker": "FED-1", "action": "buy", "side": "yes", "price": 40, "tif": "IOC"}],
    )
    assert router.execute_signal(signal)
    kalshi_bundle = reporting_repo.load_kalshi_reporting_bundle()
    statuses = [row["status"] for row in kalshi_bundle.orders]
    assert "simulated" in statuses
    assert len(kalshi_bundle.fills) == 1


def test_reporter_writes_required_report_shape(tmp_path: Path) -> None:
    db = Database(tmp_path / "report.db")
    repo = KalshiRepository(db)
    repo.persist_evaluation(
        StrategyEvaluation(
            strategy=StrategyName.CROSS_MARKET,
            ticker="FED-1",
            generated=True,
            reason="signal_generated",
            edge_before_fees=0.05,
            edge_after_fees=0.03,
            fee_estimate=0.01,
            quantity=1,
            metadata={},
        )
    )
    report_path = tmp_path / "report.json"
    report = PaperTradingReporter(ReportingRepository(db), report_path).write()
    loaded = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["cumulative"]["total_signals"] == loaded["cumulative"]["total_signals"]
    assert "run_manifest" in loaded
    assert set(["this_run", "cumulative", "markets_seen", "markets_filtered_out", "markets_loaded"]).issubset(loaded)
