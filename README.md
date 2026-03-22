# Arbitrage Machine

Multi-mode trading research repo with:
- Kalshi / prediction-market infrastructure
- belief-layer disagreement research
- event-driven news-to-trade research for liquid U.S. assets
- deterministic replay and edge-analysis tooling

## Current Posture

- Paper research: approved
- Limited paper trading: allowed only under the conservative execution baseline and the runbook checks
- Live trading: not approved

Current operator references:
- `docs/paper_trading_runbook.md`
- locally generated hardened baseline artifact:
  - `data/baselines/hardened_operator_baseline/hardened_baseline_summary.json`

The hardened paper baseline is intentionally conservative:
- buys fill at ask or worse
- sells fill at bid or worse
- displayed size is enforced
- partial fills are allowed
- passive orders do not assume instant fills
- replay is session-aware and anchored to the first tradable bar at or after the event timestamp within the same session

## Architecture

- `app/main.py`: dispatch-only entrypoint
- `app/layer2_kalshi/runtime.py`: Kalshi runtime ownership
- `app/layer3_belief/runtime.py`: belief-layer runtime ownership
- `app/common/bootstrap.py`: shared runtime/bootstrap loading
- `app/persistence/`: domain repositories plus modularized schema fragments
- `app/reporting.py`: report orchestration only
- `app/reporting_kalshi.py`: Kalshi reporting helpers
- `app/reporting_belief.py`: belief/outcome reporting helpers
- `app/reporting_event_driven.py`: event-driven manifests and summaries
- `app/replay/`: deterministic replay, canonical historical research store, feature/outcome pipeline, edge analysis

## Repo Shape

```text
arbitrage-machine/
  app/
    common/
    events/
    execution/
    ingestion/
    classification/
    mapping/
    kalshi/
    layer2_kalshi/
    layer3_belief/
    persistence/
    replay/
    reporting.py
    reporting_kalshi.py
    reporting_belief.py
    reporting_event_driven.py
  deploy/
  docs/
    paper_trading_runbook.md
  tests/
  .env.example
  pyproject.toml
```

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

## Event-Driven Paper Path

Core modules:
- `app/ingestion/`: RSS + NewsAPI ingestion
- `app/classification/`: rule-based classification with optional OpenAI enrichment
- `app/mapping/`: event-to-trade rules
- `app/events/`: event runtime
- `app/execution/broker.py`: conservative paper broker
- `app/execution/event_engine.py`: staged entries, exits, and outcome persistence
- `app/risk/event_risk.py`: per-trade sizing

Enable it with:

```powershell
ENABLE_EVENT_DRIVEN_MODE=true
EVENT_TRADING_ONLY_MODE=true
EVENT_EXECUTION_ENABLED=true
LIVE_TRADING=false
```

Conservative paper execution assumptions:
- marketable buys fill at ask, sells at bid
- displayed size is enforced
- larger orders can partially fill or walk synthetic depth at worse prices
- passive orders require a later crossing rule
- append-only paper execution audit rows are written to `event_paper_execution_audit`

Useful execution knobs:
- `EVENT_PAPER_SUBMISSION_LATENCY_SECONDS`
- `EVENT_PAPER_HALF_SPREAD_BPS`
- `EVENT_PAPER_TOP_LEVEL_SIZE`
- `EVENT_PAPER_DEPTH_LEVELS`
- `EVENT_PAPER_PASSIVE_RECHECK_SECONDS`

Paper artifacts include a versioned run manifest with:
- execution-model parameters
- config identity hash
- tracked code-file hashes for the event-driven paper path

## Replay / Backtest

Replay lives under `app/replay/` and uses a canonical local research data store.

Default storage path:
- `data/research/historical/`

Layout:
- `data/research/historical/<provider>/<symbol>/<interval>/<YYYY-MM>.json`
- `data/research/historical/<provider>/<symbol>/<interval>/manifest.json`

Behavior:
- bars are normalized once into a strict canonical schema
- repeated replay runs over overlapping ranges reuse stored chunks
- replay remains limited by the provider's returned granularity
- entry is anchored to the first bar at or after the event timestamp within the same session
- intraday horizons do not jump into the next session
- provider downgrade / realism flags propagate into outcomes and edge analysis

Run with a local JSON dataset:

```powershell
python -m app.replay.main --json C:\path\to\historical_news.json
```

Or with CSV:

```powershell
python -m app.replay.main --csv C:\path\to\historical_news.csv
```

If `EVENT_NEWSAPI_KEY` is set:

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

Replay research outputs can write:
- `data/event_replay_report.json`
- `data/research/outcomes/event_outcomes.json`
- `data/research/outcomes/event_outcomes.csv`
- `data/research/outcomes/edge_analysis_report.json`

These are research artifacts for conditional edge discovery, not live signals.

## Runbook

Short form:
1. Install dependencies and copy `.env.example` to `.env`
2. Keep `LIVE_TRADING=false`
3. Run `python -m app.preflight`
4. Run `python -m app.main`
5. Verify run manifest, audit rows, `runtime_status.json`, and conservative paper fills
6. Halt immediately on stale data, session violations, missing audit rows, stale `runtime_status.json`, or unrealistic fills

Detailed operator guidance:
- `docs/paper_trading_runbook.md`

## Remaining TODOs

- Validate live Kalshi auth details against your account keys
- Replace scraped FedWatch with a stronger licensed feed if uptime matters
- Add stronger broker reconciliation for exchange-side cancels and partial fills
- Add out-of-process monitoring if unattended multi-day uptime matters
