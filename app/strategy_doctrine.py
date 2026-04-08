from __future__ import annotations

"""Central operating doctrine distilled from local strategy research PDFs.

This module is intentionally lightweight. It does not redesign runtime logic.
It keeps the repo's strategic center of gravity explicit so config defaults,
runtime messaging, and sleeve registry descriptions stay aligned.
"""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ResearchSource:
    title: str
    filename: str
    main_claim: str


@dataclass(frozen=True, slots=True)
class SleeveDoctrine:
    sleeve_name: str
    role: str
    mandate: str
    why_it_exists: str
    scale_profile: str
    promotion_gate: str


MASTER_THESIS = (
    "Build a capital-allocation machine, not a strategy zoo: a scalable "
    "cross-asset trend core, a capacity-limited event-contract booster, a "
    "belief/calibration research layer, and keep options/convexity gated "
    "until the core engine proves durable edge."
)


RESEARCH_SOURCES: tuple[ResearchSource, ...] = (
    ResearchSource(
        title="Highest-upside automated U.S.-legal trading business for a solo builder in 2026",
        filename="Highest-upside automated U.S.-legal trading business for a solo builder in 2026.pdf",
        main_claim="ETF or futures trend with volatility targeting is the strongest scalable core; event contracts are real but lower-capacity.",
    ),
    ResearchSource(
        title="Designing a High-Upside, Fully Automated Trading System for Small Capital in U.S.-Legal Markets",
        filename="Designing a High-Upside, Fully Automated Trading System for Small Capital in U.S.-Legal Markets.pdf",
        main_claim="Prediction markets are better as belief and distribution inputs than as the only scalable execution venue.",
    ),
    ResearchSource(
        title="Building a U.S.-Legal Fully Automated Event-Contracts Trading System in 7 Days",
        filename="Building a U.S.-Legal Fully Automated Event-Contracts Trading System in 7 Days.pdf",
        main_claim="Kalshi Fed ladder monotonicity and structural verticals are the cleanest fast-start event sleeve.",
    ),
    ResearchSource(
        title="A Quant Team Playbook for U.S.-Legal Automated Event-Contract Trading",
        filename="A Quant Team Playbook for U.S.-Legal Automated Event-Contract Trading.pdf",
        main_claim="The durable moat in event contracts is cross-market belief mapping and calibration, not naive one-venue signal chasing.",
    ),
    ResearchSource(
        title="A 7-Day, Fully Automated, U.S.-Legal Equity Trading System for a Small Retail Account",
        filename="A 7-Day, Fully Automated, U.S.-Legal Equity Trading System for a Small Retail Account.pdf",
        main_claim="Week-one retail realism means low-frequency ETF trend plus risk targeting, not intraday microstructure fantasies.",
    ),
    ResearchSource(
        title="Designing a Retail-Capital, Fully Automated Options Trading System With Sustainable Positive Expecte",
        filename="Designing a Retail-Capital, Fully Automated Options Trading System With Sustainable Positive Expecte.pdf",
        main_claim="Options need real IV-surface, execution, and tail-risk infrastructure, so they should stay gated until the core engine is proven.",
    ),
    ResearchSource(
        title="Building a High-Value Product from Gauss World Vibe-Coding and Algorithmic Trading Ideas",
        filename="gauss_world_product_vibe.pdf",
        main_claim="The high-value product is not just a bot; it is a trustworthy paper-trading, verification, auditability, and deployment surface built on top of the trading kernel.",
    ),
)


SLEEVE_DOCTRINE: dict[str, SleeveDoctrine] = {
    "trend": SleeveDoctrine(
        sleeve_name="trend",
        role="core_engine",
        mandate="Primary scalable capital-allocation sleeve across liquid cross-asset ETFs.",
        why_it_exists="This is the only sleeve currently positioned to compound meaningful capital without depending on thin venue microstructure.",
        scale_profile="High relative scalability; should carry most paper risk budget and most validation effort.",
        promotion_gate="Must beat a simple passive benchmark on state changes, allocation quality, and risk-adjusted behavior before the repo deserves more complexity.",
    ),
    "kalshi_event": SleeveDoctrine(
        sleeve_name="kalshi_event",
        role="booster_sleeve",
        mandate="Opportunistic structural and event-contract alpha sleeve, especially in ladders and cross-market mispricings.",
        why_it_exists="Prediction markets can produce real edge, but depth and sizing constraints make them a booster rather than the main business.",
        scale_profile="Capacity-limited; useful for additive alpha and belief extraction, not the backbone allocator.",
        promotion_gate="Must show repeatable positive opportunity quality after fees and realistic depth checks without pretending it can absorb large size.",
    ),
    "belief": SleeveDoctrine(
        sleeve_name="belief",
        role="research_layer",
        mandate="Research and calibration layer for implied distributions, disagreement, and cross-market mapping.",
        why_it_exists="The PDFs consistently point to distribution mapping and calibration as the research moat that can feed both trend overlays and event sleeves.",
        scale_profile="Non-execution layer; scales as research and observability infrastructure rather than direct capital deployment.",
        promotion_gate="Should improve allocation or event filtering quality; it does not need standalone trading promotion.",
    ),
    "options": SleeveDoctrine(
        sleeve_name="options",
        role="gated_experimental",
        mandate="Longer-horizon options research for structural premia and convex payoffs.",
        why_it_exists="Options can matter later, but only after the repo has trustworthy volatility surface, execution, and assignment realism.",
        scale_profile="Potentially large, but operationally heavy and easy to delude yourself with if promoted too early.",
        promotion_gate="Do not promote until the trend core is proven and options infrastructure can price, risk, and execute honestly.",
    ),
    "convexity": SleeveDoctrine(
        sleeve_name="convexity",
        role="gated_experimental",
        mandate="Experimental convexity structures for tail or event-linked expression.",
        why_it_exists="Useful only if the repo already has a robust core and real options infrastructure.",
        scale_profile="High model risk and limited current readiness.",
        promotion_gate="Stay gated until options maturity and execution realism are materially stronger.",
    ),
}


def doctrine_summary() -> dict[str, object]:
    return {
        "master_thesis": MASTER_THESIS,
        "operating_hierarchy": ["trend", "kalshi_event"],
        "gated_modules": ["options", "convexity"],
        "sources": [
            {
                "title": source.title,
                "filename": source.filename,
                "main_claim": source.main_claim,
            }
            for source in RESEARCH_SOURCES
        ],
        "sleeves": {
            name: {
                "role": doctrine.role,
                "mandate": doctrine.mandate,
                "why_it_exists": doctrine.why_it_exists,
                "scale_profile": doctrine.scale_profile,
                "promotion_gate": doctrine.promotion_gate,
            }
            for name, doctrine in SLEEVE_DOCTRINE.items()
        },
    }
