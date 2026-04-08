# Belief Engine Plan

## Objective

Refactor the repo into a coherent belief arbitrage engine with:

- a survival-first trend baseline
- honest sleeve/runtime controls
- replayable belief objects
- cross-market disagreement storage
- execution-aware diagnostics and reporting

## Files To Modify

- `pyproject.toml`
- `app/config.py`
- `app/db.py`
- `app/kalshi/rest_client.py`
- `app/portfolio/runtime.py`
- `app/portfolio/orchestrator.py`
- `app/strategies/trend/models.py`
- `app/strategies/trend/portfolio.py`
- `app/persistence/contracts.py`
- `app/persistence/belief_repo.py`
- `app/persistence/reporting_repo.py`
- `app/persistence/schema/belief.py`
- `app/persistence/trend_repo.py`
- `app/reporting.py`
- `app/reporting_belief.py`
- `app/layer3_belief/runtime.py`
- `app/research/belief_objects.py`
- `app/research/disagreement_engine.py`

## Files To Create

- `app/research/normalization.py`
- `docs/repo_truth_audit.md`
- `docs/belief_engine_implementation.md`

## Data Model Changes

Add or harden:

- `belief_sources`
- `event_definitions`
- richer `belief_snapshots`
- richer `disagreement_snapshots`
- `cross_market_disagreements`
- `outcome_realizations`

Persist for each normalized belief object:

- timestamp
- source key
- event key
- normalized underlying key
- comparison metric
- transformed belief value
- payload summary
- belief summary
- quality flags
- confidence
- cost estimate

## Migration Implications

- Existing SQLite databases remain valid.
- New tables are additive.
- Existing belief/disagreement tables gain additive columns.
- No destructive migration is required.

## Test Plan

1. Verify Kalshi client remains data-only safe with injected stub sessions.
2. Verify options sleeve flag actually disables candidate participation.
3. Verify trend candidate normalization uses actual portfolio equity.
4. Verify trend snapshots persist truthful diagnostics.
5. Verify belief normalization/disagreement generation produces execution-aware rows.
6. Verify reporting removes misleading fake-actual-PnL field and exposes honest execution truth.
