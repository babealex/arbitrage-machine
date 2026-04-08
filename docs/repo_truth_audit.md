# Repo Truth Audit

## What The Repo Actually Is

The codebase is not a single trading engine. It is a paper-first multi-runtime system with:

- `app/runtime/two_sleeve.py` as the default paper runtime
- `app/portfolio/runtime.py` as a gated allocator/orchestrator runtime
- `app/events/runtime.py` as an event/news paper runtime
- `app/layer3_belief/runtime.py` as a research/belief collection runtime
- `app/replay/` as replay and research infrastructure

Persistence is SQLite via `app/db.py` plus modular schema fragments under `app/persistence/schema/`.

## Verified Architectural Conflicts Found

### 1. Sleeve truthfulness was incomplete

- `ENABLE_OPTIONS_SLEEVE` and `ENABLE_CONVEXITY_SLEEVE` existed in config.
- The orchestrator still constructed options and convexity engines unconditionally.
- `OptionsCandidateEngine` did not check its sleeve flag.
- Convexity used `CONVEXITY_ENABLED` while sleeve registration used `ENABLE_CONVEXITY_SLEEVE`.

Result: documented disabled sleeves could still participate.

### 2. Trend sizing was not consistently capital-aware

- `TrendPaperEngine` already used live snapshot equity in its own run loop.
- `PortfolioOrchestrator._normalize_trend_candidate()` did not.
- It hardcoded `100000` when computing trend candidate EV/risk/notional.

Result: allocator scoring and sizing could be distorted away from actual account equity.

### 3. Reporting had a misleading PnL surface

- `PaperTradingReporter` emitted `estimated_vs_actual_pnl`.
- That field never computed actual realized PnL; it always wrote zero for actual.
- The same report already had a more honest `execution_report` built from execution outcomes.

Result: operator-facing output implied truth it did not have.

### 4. Belief layer existed, but only as a prototype

The repo already had:

- `belief_snapshots`
- `disagreement_snapshots`
- `outcome_tracking`

But the stored objects were too narrow:

- heavily Kalshi-shaped
- weak source/event normalization
- no explicit belief sources table
- no explicit event definitions table
- no persisted cross-market disagreement table with execution-aware costs
- no outcome realization table

### 5. Dependency injection was brittle

`KalshiRestClient` assumed any injected session had `.headers`, which broke a valid test double.

### 6. Packaging understated the real module graph

`pyproject.toml` listed only `packages = ["app"]`, which was not truthful to the actual repo layout with many subpackages.

## Current Strategic Fit

### Strongest existing foundations

- Trend sleeve infrastructure already exists and is the right survival-first baseline.
- Replay/data persistence shape is compatible with a research flywheel.
- Belief/disagreement runtime already exists as a natural seed for a broader belief arbitrage layer.
- Execution outcomes, market state snapshots, and risk events are already persisted.

### Weakest existing foundations

- Sleeve gating was not honest enough.
- Cross-market belief normalization was too loose.
- Reporting mixed honest execution metrics with placeholder-style fields.
- Some runtime diagnostics existed only indirectly and were not consistently attached to persisted trend state.

## Truthful Repo Statement After This Refactor

The repo is now best described as:

> A survival-first paper trading and research stack with a capital-aware trend base engine, a replayable belief-object store, and a disagreement research layer for studying when markets disagree about the same underlying reality after costs.

That is more accurate than calling it a generic multi-sleeve arbitrage machine.
