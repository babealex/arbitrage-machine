# Paper Trading Runbook

This repo is approved for paper research and limited paper trading only under the hardened baseline.

Paper validation is not execution proof. Cleaner numerics do not remove the main remaining risks: stale books, disconnects, legging, fee underestimation, partial-fill path dependence, and exchange-side state drift.

## Scope

- Event-driven paper execution uses the conservative fill model:
  - buys fill at ask or worse
  - sells fill at bid or worse
  - displayed size is enforced
  - partial fills are allowed
  - passive orders do not assume instant fills
- Replay uses session-aware entry blocking and first-bar-at-or-after-event anchoring within the same tradable session.
- Live trading remains disabled unless explicitly enabled.

## Pre-Start Checks

1. Confirm `.env` or runtime config has:
   - `LIVE_TRADING=false`
   - `ENABLE_EVENT_DRIVEN_MODE=true` only if running the event path
   - `EVENT_EXECUTION_ENABLED=true` only for paper execution
2. Confirm writable paths exist:
   - `data/`
   - `logs/`
   - `data/research/historical/`
   - `data/runtime_status.json`
3. Run:
   - `python -m app.preflight`
4. Confirm disk headroom is available before replay or long paper runs.
4. Confirm the latest validation baseline still passes:
   - runtime imports
   - persistence repo tests
   - event execution regressions
   - conservative paper execution comparison
   - replay edge regressions
   - replay integration

## No-Trade Conditions

Do not run paper trading if any of the following are true:

- stale market data
- stale or invalid external confirmation data in balanced/aggressive modes
- reconciliation failures
- abnormal quote completeness
- market closed for the target asset session
- edge does not exceed estimated fees plus execution buffer by a wide margin
- the target market is too close to expiry for orderly entry and exit
- orderbook depth is inadequate for the intended size
- disconnects or sequence gaps leave book state uncertain
- research replay relying on coarse bars for short horizons

## Operator Procedure

1. Start in paper mode only.
2. Watch the first report artifact and confirm:
   - run manifest exists
   - execution model parameters are present
   - paper execution audit rows are being written
   - rejected and unfilled decisions appear when expected
3. Check append-only audit rows before trusting any PnL number.
4. For replay conclusions, treat coarse-bar or downgraded-provider rows as lower quality.

## Numeric Guardrails

Production trade gating should be read as:

- `edge_after_costs = edge_before_fees - estimated_fee_drag - execution_buffer`
- only trade when `edge_after_costs` exceeds the configured threshold
- reject if stale data, insufficient depth, expiry proximity, or state uncertainty weakens the quote context

Paper and replay analytics still use simplified internal representations in some paths. Do not treat report smoothness or summary ratios as evidence that exchange-facing fixed-point handling is complete.

## Required Artifacts Per Run

- report JSON
- SQLite DB
- structured log
- runtime status artifact
- run manifest with execution-model identity
- append-only event paper execution audit rows

## Kill / Halt Conditions

Stop the paper run immediately if:

- audit rows stop appearing
- report manifest is missing
- orders appear to fill outside session
- passive orders start filling without a crossing rule
- replay starts using next-session bars for intraday horizons
- data store writes fail repeatedly
- `runtime_status.json` stops updating or reports `state=error`

## Baseline Expectations

The hardened baseline is intentionally harsh:

- trade count should be lower than the old optimistic model
- fill rate should be lower
- partial-fill frequency should be non-zero in stressed scenarios
- paper PnL should be materially lower than the legacy optimistic path

If a new change improves paper results without a clear structural reason, assume the change is lying until proven otherwise.

## Unattended Operation Notes

- `runtime_status.json` is the first file to inspect on restart or after a suspected stall.
- `last_successful_cycle_utc` should advance every healthy loop.
- `last_error_message` should be empty during steady-state operation.
- If preflight fails on disk headroom, fix storage first. Do not suppress the check to force a long run.
