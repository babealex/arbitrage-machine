# Belief Engine Implementation Note

## What Changed

- Sleeve gating was made truthful in the orchestrator.
- Trend normalization now uses actual portfolio equity instead of a hardcoded placeholder notional.
- Trend snapshots now persist diagnostics and kill-switch state.
- Belief storage was widened into normalized belief objects with explicit source/event tables.
- Cross-market disagreements now include cost-aware net disagreement and tradeability metadata.
- Outcome realizations are now logged separately from pending outcome tracking.
- Reporting now separates estimated edge from actual execution truth and no longer pretends to have per-strategy actual PnL when it does not.

## What Is Now True

- Disabled sleeves do not silently participate in orchestrator candidate generation.
- Convexity enablement is tied back to the sleeve truth surface instead of floating as a separate hidden switch.
- Trend allocator inputs reflect actual stored portfolio equity.
- Trend runtime diagnostics are replay-friendly because they are attached to persisted snapshots.
- Belief objects are no longer only ephemeral runtime rows; they are structured research records with normalized keys.
- Cross-market disagreement rows explicitly model gross disagreement, cost drag, and tradeability.
- Reporting exposes execution truth from persisted outcomes instead of a fake `actual_pnl = 0` surface.

## Intentionally Still Unimplemented

- No new live broker capability was invented.
- No text-to-trade direct path was added.
- No claim is made that options beliefs are yet sufficient for live deployment; they are stored as research summaries.
- No ML layer was added.

## Highest-Value Next Step

Use the new normalized belief store to build a deterministic replay study that answers:

> For each normalized underlying key and comparison metric, which source is wrong most often after fees and realistic execution buffers?

That is the shortest path from research logging to a genuine belief arbitrage product surface.
