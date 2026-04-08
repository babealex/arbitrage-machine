# Arbitrage Machine

Paper-first multi-sleeve prop-trading research and execution platform.

Current repo identity:
- trend as the production-paper backbone sleeve
- Kalshi / event structural trading as the opportunistic booster sleeve
- belief / replay as research and calibration infrastructure
- options / convexity / broker adapters as experimental surfaces

The central strategic doctrine now lives in:
- `docs/MASTER_STRATEGY_SYNTHESIS.md`
- `app/strategy_doctrine.py`

## Runtime Truth

- Default runtime: `two_sleeve_paper`
- Production-paper defaults: `trend` enabled, `kalshi_event` enabled, `options` disabled, `convexity` disabled
- Portfolio orchestrator runtime: opt-in only with `ENABLE_PORTFOLIO_ORCHESTRATOR=true`
- Live trading: not approved as a repo-wide default posture
- Supported measurement run modes:
  - `live_paper`: real data fetch worked this run
  - `replay_paper`: sleeve ran from explicit cached/persisted paper data
  - `data_unavailable`: sleeve could not operate because data access or prerequisites failed

The default operating shape is intentionally two-sleeve:
- `trend` = core engine
- `kalshi_event` = secondary alpha booster

That operating hierarchy is not arbitrary. It is the repo-level synthesis of the local strategy PDFs under `docs/pdf_extracted/`: scalable cross-asset trend first, event contracts as a smaller booster, belief/replay as calibration infrastructure, and options/convexity kept gated until the core is proven.

## Maturity Tiers

| Surface | Maturity | Notes |
| --- | --- | --- |
| `app/layer2_kalshi/runtime.py` | `stable` | Specialized Kalshi structural runtime |
| `app/runtime/two_sleeve.py` | `active_core` | Default trend + Kalshi paper operating runtime |
| `app/events/runtime.py` | `stable` | Event-driven paper execution runtime |
| `app/layer3_belief/runtime.py` | `stable` | Belief/disagreement research runtime |
| `app/replay/` | `stable` | Replay and calibration infrastructure |
| `app/allocation/` | `active_core` | Intended portfolio allocation core |
| `app/portfolio/runtime.py` | `active_core` | Explicitly gated orchestrator runtime |
| `app/strategies/trend/` | `active_core` | Trend sleeve for orchestrator core |
| `app/options/` | `experimental` | Not repo-defining health |
| `app/strategies/convexity/` | `experimental` | Package-based convexity work |
| `app/brokers/ibkr_trend_adapter.py` | `experimental` | Live broker adapter work |
| `app/strategies/convexity_engine.py` | `deprecated` | Old convexity interface retired |

The central maturity registry lives in `app/maturity.py`.

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

Important limitation:
- paper validation is not live-execution proof; the remaining risks are still stale state, disconnects, legging, fee underestimation, and exchange-side behavior drift

## Architecture

- `app/main.py`: dispatch-only entrypoint
- `app/layer2_kalshi/runtime.py`: Kalshi runtime ownership
- `app/layer3_belief/runtime.py`: belief-layer runtime ownership
- `app/events/runtime.py`: event-driven paper runtime ownership
- `app/portfolio/runtime.py`: opt-in portfolio orchestrator runtime
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
    allocation/
    kalshi/
    layer2_kalshi/
    layer3_belief/
    options/
    persistence/
    portfolio/
    replay/
    strategies/
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

Explicitly enable the active-core orchestrator path with:

```powershell
set ENABLE_PORTFOLIO_ORCHESTRATOR=true
python -m app.main
```

Run one two-sleeve paper cycle without starting the web server:

```powershell
python -m app.runtime.two_sleeve --once --no-server
```

Run a bounded measurement session for 3 cycles:

```powershell
python -m app.runtime.two_sleeve --cycles 3 --no-server
```

Bounded runs now also write a multi-cycle research summary with rejection composition, repeated near-trade candidates, and a trend-focus diagnosis:

```text
data/multi_cycle_research_report.json
```

Run the trend-only replay-diversity comparison check:

```powershell
python -m app.runtime.two_sleeve --trend-replay-diversity-check --cycles 5
```

This writes:

```text
data/trend_replay_diversity_report.json
```

Seed deterministic trend replay bars and immediately run one replay-paper measurement cycle:

```powershell
$env:TREND_DATA_MODE='cached'
python -m app.runtime.two_sleeve --seed-trend-bootstrap --once --no-server
```

Run the two-sleeve paper runtime with the localhost control panel:

```powershell
python -m app.main
```

Default control panel URL:

```text
http://127.0.0.1:8765/
```

## Data Access

Trend data controls:
- `TREND_DATA_MODE=auto|live|cached`
- `TREND_SYMBOLS=SPY,QQQ,IWM,EFA,EEM,TLT,IEF,TIP,LQD,HYG,GLD,DBC`
- `TREND_WEIGHT_MODE=current|soft_monotone|rank_normalized|gentle_score`
- `TREND_FETCH_RETRIES`
- `TREND_FETCH_BACKOFF_SECONDS`
- `TREND_FETCH_TIMEOUT_SECONDS`

Trend mode behavior:
- `auto`: try live fetch first, fall back to persisted `daily_bars` only if they already exist
- `live`: require live fetch success
- `cached`: explicit replay/bootstrap mode from persisted bars only
- `gentle_score` is now the default trend allocator shape for the paper runtime because it produces more interior weights than the old cap-saturating inverse-vol mapping

Replay/bootstrap note:
- `--seed-trend-bootstrap` writes a deterministic 30-bar daily history for the configured trend symbols into the active database so the trend sleeve can produce replay-paper opportunities and ledger rows immediately
- the default bootstrap universe is now a small cross-asset ETF set rather than a 3-symbol `SPY/QQQ/TLT` toy allocator, so the trend diagnostics can test real breadth before any broader redesign

Kalshi data controls:
- `KALSHI_DATA_MODE=auto|live|cached`
- `KALSHI_API_URL`
- `KALSHI_API_KEY_ID`
- `KALSHI_PRIVATE_KEY_PATH`

Kalshi mode behavior:
- `auto` and `live`: use live market discovery / orderbook access
- `cached`: currently treated as unsupported and surfaced explicitly as unavailable, not silently faked

Auth notes:
- Kalshi market discovery can run in data-only mode without auth when the API is reachable
- order placement auth still requires `KALSHI_API_KEY_ID` and `KALSHI_PRIVATE_KEY_PATH`

## Measurement Artifacts

The two-sleeve runtime now writes:
- `data/control_panel_state.json`
- `data/paper_report.json`
- `data/first_measurement_report.json`
- `data/multi_cycle_research_report.json`
- `data/strategy_transparency_pack.json`

These operator artifacts now also carry the repo doctrine explicitly:
- the master thesis
- the operating hierarchy
- the benchmark question: `What can this machine do that a simple SPY/QQQ allocator cannot?`
- per-sleeve role, mandate, scale profile, and promotion gate

The strategy transparency pack is the product-facing trust artifact inspired by the Gauss World product PDF. It records:
- what the system does
- the commercial wedge (`paper_trading_plus_alerts_plus_verification`)
- the assumptions and data modes in force
- operator disclosures
- the active sleeves and their current run modes

These artifacts expose, per sleeve:
- run mode
- opportunities seen
- trades attempted
- trades filled
- fill rate
- expected vs realized edge
- multi-cycle average expected edge
- multi-cycle average realized edge
- multi-cycle expected vs realized gap
- best edge seen
- fees and slippage
- no-trade reasons
- repeated near-trade candidates
- recent errors and blocker notes

Trend bounded runs also expose a `trend_focus` summary with:
- signal cycle rate
- target cycle rate
- average targets generated
- average signals generated
- average gross exposure ratio
- average cash ratio
- average position count
- average drawdown
- a simple interpretation such as `already_positioned_not_retriggering`, `underdeployed_without_rebalance_triggers`, or `active_rebalancing`

Bounded runs now also expose a separate `trend_sparsity` attribution block that answers why the trend sleeve is sparse:
- whether the replay/bootstrap data stayed too static across cycles
- whether the target universe barely changed
- whether the portfolio was already near target
- whether rebalance deltas existed but were mostly suppressed by the rebalance threshold
- whether runtime/risk constraints were the main suppressor

The main `trend_sparsity.interpretation` values are:
- `bootstrap_too_smooth`
- `threshold_too_wide`
- `universe_too_stable`
- `already_at_target`
- `constraint_suppressed`
- `mixed_sparse`
- `insufficient_data`

The replay-diversity check compares:
- `cached_bootstrap_static`: current cached bootstrap behavior with no forced advancement
- `cached_bootstrap_shifted`: deterministic cached replay with advancing input windows

The operational question is simple:
- if target drift, target turnover, or signal rate increase in the shifted scenario, the replay environment is the bottleneck
- if they do not, trend sparsity is more likely genuine strategy/universe behavior

Trend bounded runs also expose a `trend_construction` block so you can tell whether the target-building math itself is keeping the sleeve static:
- how many symbols have positive momentum
- how many repeatedly bind at `max_weight`
- whether gross raw target weight is being scaled down
- how concentrated the final target mix is

Bounded runs now also expose a `trend_validation` block for the high-level business question:
- is the allocator still structurally saturated
- is it diversified but still too low-drift to matter
- or is it showing meaningful bounded responsiveness

The main `trend_validation.interpretation` values are:
- `construction_still_saturated`
- `expressive_but_low_drift`
- `responsive_allocator_with_light_rebalance`
- `mixed_validation`
- `insufficient_data`

Bounded runs also expose a `trend_signal` block so you can tell whether the momentum inputs themselves are strong enough to justify rotation:
- whether most assets are simply drifting positive together with little separation
- whether there is meaningful cross-sectional rank spread
- or whether the signal regime is just weak

Bounded runs now also expose a `trend_benchmark` block so the repo stops asking the passive-benchmark question without measuring it. The current benchmark pass is deliberately simple and auditable:
- strategy compounded return over consecutive trend snapshot intervals
- equal-weight `SPY/QQQ` compounded return over the same intervals when both closes are available
- equal-weight full trend-universe return over the same intervals when enough closes are available
- interval outperformance rate versus each passive baseline

This benchmark block is in-sample and bounded to the observed replay/live-paper window. It is meant to answer whether the current trend sleeve is doing anything meaningfully different from a cheap passive allocator, not to claim a production-grade backtest.

If replay inputs advance but `trend_construction` still shows the same small positive-momentum core repeatedly binding at `max_weight`, the bottleneck is no longer replay smoothness. It is the current universe / weighting construction.

The replay-diversity artifact now also includes a `weight_shape_sensitivity` block for each scenario. This compares the current allocator against a few bounded alternative weight mappings on the same persisted momentum/vol inputs and reports whether cap binding, concentration, and effective-name count improve before any live strategy change is made.

There is now also a bounded `trend_threshold_sensitivity_report.json` path for the next decision gate. It runs the same shifted replay inputs across a few `TREND_MIN_REBALANCE_FRACTION` settings and reports whether lower rebalance thresholds actually improve benchmark-relative posture or just create more motion:
- current 10% rebalance threshold
- 5% rebalance threshold
- 2% rebalance threshold

The decision standard stays narrow:
- if lower thresholds improve `SPY/QQQ` excess return materially, rebalance sensitivity is likely the next real lever
- if they do not, threshold tweaking is not the answer and the bottleneck is elsewhere

How to read an empty ledger:
- empty ledger + opportunities/no-trade reasons means the sleeve ran but found no executable trade
- empty ledger + `data_unavailable` means the sleeve was starved by data access or prerequisites
- empty ledger + `replay_paper` means cached data existed but did not produce an attempted trade in that run

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

Trade gating is intended to remain conservative:
- `edge_after_costs = edge_before_fees - estimated_fee_drag - execution_buffer`
- no trade near expiry, on stale/uncertain book state, or when depth is inadequate

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
