# Master Strategy Synthesis

This note is the single repo-local synthesis of the six downloaded strategy PDFs in [docs/pdf_extracted](C:\Users\alexa\arbitrage-machine\docs\pdf_extracted).

## Core Conclusion

The PDFs point to one coherent operating model:

- `trend` should be the scalable core engine
- `kalshi_event` should be a smaller booster sleeve
- `belief / replay / calibration` should be treated as research infrastructure and distribution-mapping moat
- `options / convexity` should stay gated until the core engine is proven and the options stack is much more mature

This is the same question the repo must keep answering:

`What can this machine do that a simple SPY/QQQ allocator cannot?`

If the answer is "not much," the extra complexity is not justified.

## What Each PDF Adds

### Highest-upside automated U.S.-legal trading business for a solo builder in 2026

- Best scalable core for a solo builder is low-frequency ETF or futures trend with risk targeting.
- Event contracts can be real, but they are capacity-limited.
- The repo implication is straightforward: `trend` earns the majority of development and validation effort.

### Designing a High-Upside, Fully Automated Trading System for Small Capital in U.S.-Legal Markets

- Durable edge comes from implied-distribution thinking, cross-market belief mapping, and slow information diffusion.
- Prediction markets should not be treated as the only scalable venue; they are also useful as data and calibration inputs.
- Repo implication: keep `belief` and cross-market calibration as research infrastructure, not decorative extras.

### Building a U.S.-Legal Fully Automated Event-Contracts Trading System in 7 Days

- The cleanest week-one Kalshi path is structural Fed ladder monotonicity / vertical logic.
- Repo implication: current Kalshi ladder, partition, and structural-event focus is directionally correct.

### A Quant Team Playbook for U.S.-Legal Automated Event-Contract Trading

- The real event-contract moat is better mapping between prediction markets, macro proxies, surveys, and implied distributions.
- Repo implication: Kalshi should stay tied to calibration and cross-market context, not become a standalone "trade everything" universe.

### A 7-Day, Fully Automated, U.S.-Legal Equity Trading System for a Small Retail Account

- A realistic retail V1 is low-frequency ETF trend plus risk targeting.
- Intraday microstructure dreams, quote-heavy strategies, and premature options complexity are a trap.
- Repo implication: the current trend-first posture is justified, but only if the allocator becomes meaningfully broader and more responsive than a toy ETF bot.

### Designing a Retail-Capital, Fully Automated Options Trading System With Sustainable Positive Expecte

- Sustainable options automation requires a real IV-surface, Greeks, EV, assignment, and execution stack.
- Repo implication: keep options and convexity gated. Promoting them before the core is proven would add fragility, not business quality.

### Building a High-Value Product from Gauss World Vibe-Coding and Algorithmic Trading Ideas

- A high-value trading product is not just a bot. It is a trustworthy strategy-to-production surface with auditable artifacts, data caveats, paper-trading verification, monitoring, and clear operator disclosures.
- Repo implication: the trading kernel needs first-class trust surfaces. The system should emit explicit transparency artifacts that explain what it does, under which assumptions, with which data modes and operator responsibilities.

## What Survives Contact With The Current Repo

These conclusions are already supported by current diagnostics:

- `trend` is the only plausible engine candidate
- `kalshi_event` is alive and research-useful, but capacity-limited
- replay smoothness was a real issue, but not the only one
- a 3-symbol trend universe was too narrow
- a cap-saturating allocator was too blunt
- after broadening the universe and fixing allocator saturation, the next bottleneck appears to be trend responsiveness and rebalance sensitivity

So the repo should be interpreted as:

- a capital-allocation machine under construction
- not a narrow "arbitrage bot"
- not a strategy zoo where every sleeve gets equal billing

## Concrete Repo Doctrine

### Trend

- Primary job: scalable cross-asset allocation
- Validation standard: must justify itself against a simple passive benchmark, not just look sophisticated
- First promotion gate: prove meaningful state changes and outcome quality with the broadened universe and improved allocator

### Kalshi/Event

- Primary job: opportunistic booster sleeve and source of belief / distribution information
- Validation standard: realistic depth, fee, and sizing-aware opportunity quality
- Promotion gate: prove positive expected edge after realistic microstructure costs without pretending to absorb large size

### Belief / Replay

- Primary job: research, calibration, and regime context
- Validation standard: improves decisions in other sleeves
- Promotion gate: does not need standalone profitability if it materially improves core allocation or event filtering

### Options / Convexity

- Primary job: later-stage structured exposure once the core engine is already worth amplifying
- Validation standard: real pricing, risk, and execution realism
- Promotion gate: stay off the default path until the core is proven and the options surface is not a fantasy

## What This Means Operationally

The repo should keep enforcing:

- `trend` as the default core sleeve
- `kalshi_event` as the secondary booster sleeve
- `options` and `convexity` disabled by default
- research output that explains whether complexity is earning its keep
- trust artifacts that explain what the system does, what assumptions it is making, and why paper results should not be mistaken for live-readiness

The next serious benchmark remains:

`Can the improved trend sleeve do something materially better than a simple passive ETF allocator?`

Until that answer is yes, the repo should resist sideways expansion.
