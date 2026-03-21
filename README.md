# Arbitrage Machine

Multi-mode trading research repo with:
- Kalshi / prediction-market infrastructure
- belief-layer disagreement research
- event-driven news-to-trade pipeline for liquid U.S. assets

## Repo Tree

```text
arbitrage-machine/
├── app/
│   ├── __init__.py
│   ├── alerts/
│   │   ├── __init__.py
│   │   └── notifier.py
│   ├── config.py
│   ├── db.py
│   ├── execution/
│   │   ├── __init__.py
│   │   ├── orders.py
│   │   └── router.py
│   ├── external_data/
│   │   ├── __init__.py
│   │   └── fed_futures.py
│   ├── kalshi/
│   │   ├── __init__.py
│   │   ├── rest_client.py
│   │   └── ws_client.py
│   ├── logging_setup.py
│   ├── main.py
│   ├── market_data/
│   │   ├── __init__.py
│   │   ├── ladder_builder.py
│   │   ├── orderbook.py
│   │   └── service.py
│   ├── models.py
│   ├── portfolio/
│   │   ├── __init__.py
│   │   └── state.py
│   ├── risk/
│   │   ├── __init__.py
│   │   └── manager.py
│   └── strategies/
│       ├── __init__.py
│       ├── base.py
│       ├── cross_market.py
│       ├── momentum_stub.py
│       ├── monotonicity.py
│       └── partition.py
├── deploy/
│   ├── arbitrage-machine.plist
│   └── arbitrage-machine.service
├── tests/
│   ├── test_cross_market.py
│   ├── test_fee_and_orderbook.py
│   ├── test_integration_mocked_kalshi.py
│   ├── test_ladder_and_monotonicity.py
│   ├── test_partition.py
│   └── test_risk_rules.py
├── .env.example
├── pyproject.toml
└── README.md
```

## Architecture

- `app/config.py`: env-driven runtime config and hard safety limits.
- `app/kalshi/`: REST and WebSocket adapters.
- `app/market_data/`: orderbook normalization and ladder grouping.
- `app/external_data/`: stale-checked external probabilities.
- `app/strategies/`: signal generation modules.
- `app/execution/`: idempotent order creation and guarded leg execution.
- `app/risk/`: kill conditions and exposure rules.
- `app/portfolio/`: broker reconciliation.
- `app/db.py`: SQLite persistence.
- `app/main.py`: unattended trading loop and heartbeat.

## Install

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -e .[dev]
copy .env.example .env
```

## Run

```powershell
python -m app.main
```

## Event-Driven V1

The repo now includes a separate event-driven system that turns real-time news into paper-tradable U.S. ETF / equity proxy ideas.

Core modules:
- `app/ingestion/`: RSS + NewsAPI ingestion
- `app/classification/`: rule-based event classification with optional OpenAI enrichment
- `app/mapping/`: hardcoded, versioned event-to-trade rules
- `app/events/`: in-process event bus + event runtime
- `app/execution/broker.py`: paper broker + market proxy book
- `app/execution/event_engine.py`: staged entries, exits, and trade outcome persistence
- `app/risk/event_risk.py`: simple per-trade sizing

Enable it with:

```powershell
ENABLE_EVENT_DRIVEN_MODE=true
EVENT_TRADING_ONLY_MODE=true
EVENT_EXECUTION_ENABLED=true
```

Then run:

```powershell
python -m app.main
```

Example flow:
1. RSS / NewsAPI headline arrives.
2. Rule classifier tags the event as `geopolitical`, `macro`, `earnings`, `supply_shock`, or `regulation`.
3. Mapping engine turns it into trade ideas such as `buy XOM` / `sell DAL`.
4. Confirmation layer checks proxy market moves like `SPY`, `QQQ`, `TLT`, `^VIX`, `CL=F`, `UUP`.
5. Passing ideas are sized, queued in tranches, and paper-executed conservatively:
   - marketable buys fill at ask, sells at bid
   - displayed top-of-book size is respected
   - larger orders can partially fill or walk synthetic depth at worse prices
   - passive orders do not assume instant fills
   - execution audit rows are written append-only to `event_paper_execution_audit`
6. Filled positions are later exited by stop or time.
7. Outcomes are stored in SQLite and summarized in the report JSON.

Useful paper-execution knobs:
- `EVENT_PAPER_SUBMISSION_LATENCY_SECONDS`
- `EVENT_PAPER_HALF_SPREAD_BPS`
- `EVENT_PAPER_TOP_LEVEL_SIZE`
- `EVENT_PAPER_DEPTH_LEVELS`
- `EVENT_PAPER_PASSIVE_RECHECK_SECONDS`

Paper-trading artifacts now include a versioned run manifest with:
- execution-model parameters
- config identity hash
- tracked code-file hashes for the event-driven paper path

That manifest is written into the report artifact so paper results can be tied
back to the exact conservative execution model that produced them.

## Replay / Backtest

The repo also includes a separate replay pipeline for historical validation of the event-driven strategy.

Files:
- `app/replay/news_loader.py`
- `app/replay/data_models.py`
- `app/replay/providers.py`
- `app/replay/data_store.py`
- `app/replay/market_data.py`
- `app/replay/engine.py`
- `app/replay/simulator.py`
- `app/replay/report.py`
- `app/replay/main.py`

Replay now uses a canonical local research data store, not ad hoc request-window caching.

Default storage path:
- `data/research/historical/`

Layout:
- `data/research/historical/<provider>/<symbol>/<interval>/<YYYY-MM>.json`
- `data/research/historical/<provider>/<symbol>/<interval>/manifest.json`

Behavior:
- bars are normalized once into a strict canonical schema
- chunks are organized by provider / symbol / interval / month
- repeated replay runs over overlapping ranges reuse stored chunks
- replay remains limited by the provider's returned granularity, but reruns become deterministic at the bar-input layer
- replay now uses explicit anchoring:
  - entry = first bar at or after the event timestamp
  - horizons = first bar at or after entry plus 5m / 15m / 60m
  - provider downgrade / nearest-bar realism flags are carried into the outcome dataset and edge analysis

Run with a local JSON dataset:

```powershell
python -m app.replay.main --json C:\path\to\historical_news.json
```

Or with a CSV dataset:

```powershell
python -m app.replay.main --csv C:\path\to\historical_news.csv
```

If `EVENT_NEWSAPI_KEY` is set in `.env`, you can also replay against NewsAPI historical search:

```powershell
python -m app.replay.main --from 2026-03-01T00:00:00+00:00 --to 2026-03-07T00:00:00+00:00
```

Research data controls:

```powershell
python -m app.replay.main --json C:\path\to\historical_news.json --data-dir C:\path\to\research_store
python -m app.replay.main --json C:\path\to\historical_news.json --refresh-data
python -m app.replay.main --json C:\path\to\historical_news.json --cache-only
python -m app.replay.main --json C:\path\to\historical_news.json --ephemeral
```

Compatibility flags from the earlier replay version are still accepted:
- `--cache-dir` acts as an alias for `--data-dir`
- `--refresh-cache` acts as an alias for `--refresh-data`
- `--no-cache` disables persistence for that run, similar to `--ephemeral`

Replay output is written to:
- `data/event_replay_report.json`

Replay research outputs can also write:
- `data/research/outcomes/event_outcomes.json`
- `data/research/outcomes/event_outcomes.csv`
- `data/research/outcomes/edge_analysis_report.json`

These are research artifacts for conditional edge discovery:
- the outcome dataset is a flat `event x symbol x horizon` table
- the edge analysis report ranks transparent setup slices such as event type + symbol + severity bucket
- this is not a black-box predictor and not a live signal generator

Feature / edge-analysis controls:

```powershell
python -m app.replay.main --json C:\path\to\historical_news.json --write-outcomes --write-edge-analysis
python -m app.replay.main --json C:\path\to\historical_news.json --analysis-output-dir C:\path\to\research_outputs
python -m app.replay.main --json C:\path\to\historical_news.json --min-samples 10
```

Current limitations:
- replay granularity still depends on the provider's returned bars
- coarse-bar events should be treated cautiously in the ranked setup output
- these reports are for research triage and setup discovery, not production trading decisions

Order auditability:
- exchange-style order state is now tracked append-only via `order_events`
- the `orders` table remains as the latest-state summary for compatibility

To clear and rebuild the local research store, delete:
- `data/research/historical/`

Expected local dataset fields:
- `timestamp` or `timestamp_utc`
- `headline` or `title`
- `source`
- optional `body`, `url`, `external_id`

## Runbook

1. Install dependencies and copy `.env.example` to `.env`.
2. Set `LIVE_TRADING=false` and provide Kalshi credentials plus external data settings.
3. Run `python -m app.main` in demo mode and review logs plus SQLite rows.
4. Confirm stale external data triggers a halt rather than orders.
5. Validate signals for several sessions, then enable `APP_MODE=safe`.
6. Only after safe-mode validation should `APP_MODE=balanced` be enabled for S3.

Detailed operator guidance now lives in:
- `docs/paper_trading_runbook.md`

Current posture:
- paper research: approved
- limited paper trading: only under the conservative execution baseline and with the runbook checks above
- live trading: not approved

## Remaining TODOs

- Validate live Kalshi auth details against your account keys.
- Replace scraped FedWatch with a stronger licensed feed if uptime matters.
- Add stronger broker reconciliation for exchange-side cancels and partial fills.
- Add out-of-process monitoring if you expect unattended multi-day uptime.
