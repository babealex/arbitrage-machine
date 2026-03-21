from __future__ import annotations

from app.events.models import ClassifiedEvent, NewsItem


REGION_KEYWORDS = {
    "middle east": "Middle East",
    "israel": "Middle East",
    "iran": "Middle East",
    "gaza": "Middle East",
    "china": "China",
    "taiwan": "Asia",
    "japan": "Japan",
    "europe": "Europe",
    "eurozone": "Europe",
    "u.s.": "United States",
    "fed": "United States",
}


def classify_with_rules(item: NewsItem) -> ClassifiedEvent | None:
    text = f"{item.headline}\n{item.body}".lower()
    tags: list[str] = []
    event_type = ""
    region = "Global"
    severity = 0.0
    bias = "mixed"
    assets: list[str] = []
    rationale = []

    for key, mapped in REGION_KEYWORDS.items():
        if key in text:
            region = mapped
            break

    if any(term in text for term in ("missile", "airstrike", "attack", "sanction", "conflict", "drone")):
        event_type = "geopolitical"
        severity = 0.8
        bias = "mixed"
        tags.append("energy_risk")
        rationale.append("geopolitical_escalation")
    elif any(term in text for term in ("cpi", "inflation", "pce", "core prices")):
        event_type = "macro"
        severity = 0.7
        if any(term in text for term in ("hotter than expected", "hot inflation", "accelerates", "rises unexpectedly")):
            bias = "bearish"
            tags.append("inflation_hot")
            rationale.append("inflation_hot")
        elif any(term in text for term in ("cooler than expected", "soft inflation", "disinflation", "falls unexpectedly")):
            bias = "bullish"
            tags.append("inflation_cool")
            rationale.append("inflation_cool")
        else:
            bias = "mixed"
    elif any(term in text for term in ("fed", "fomc", "rate cut", "rate hike", "treasury yield")):
        event_type = "macro"
        severity = 0.65
        if any(term in text for term in ("rate hike", "hawkish", "higher for longer")):
            bias = "bearish"
            tags.append("hawkish_rates")
        elif any(term in text for term in ("rate cut", "dovish", "easing")):
            bias = "bullish"
            tags.append("dovish_rates")
    elif any(term in text for term in ("guidance", "earnings", "revenue", "eps")):
        event_type = "earnings"
        severity = 0.55
        if any(term in text for term in ("beats", "raises guidance", "strong demand")):
            bias = "bullish"
        elif any(term in text for term in ("misses", "cuts guidance", "weak demand")):
            bias = "bearish"
    elif any(term in text for term in ("export ban", "strike", "outage", "production halt", "supply disruption")):
        event_type = "supply_shock"
        severity = 0.75
        bias = "mixed"
        tags.append("supply_shock")
    elif any(term in text for term in ("regulator", "antitrust", "ban", "tariff", "rule proposal")):
        event_type = "regulation"
        severity = 0.6
        bias = "mixed"

    if not event_type:
        return None

    if event_type == "geopolitical":
        assets = ["XLE", "XOM", "CVX", "LMT", "DAL", "UAL"]
    elif "inflation_hot" in tags or "hawkish_rates" in tags:
        assets = ["SPY", "QQQ", "TLT", "UUP"]
    elif "inflation_cool" in tags or "dovish_rates" in tags:
        assets = ["SPY", "QQQ", "TLT"]
    elif event_type == "supply_shock":
        assets = ["XLE", "XOM", "CVX"]
    elif event_type == "earnings":
        assets = ["QQQ", "SPY"]
    else:
        assets = ["SPY", "QQQ", "TLT"]

    if any(term in text for term in ("breaking", "urgent", "surge", "plunge", "emergency")):
        severity = min(1.0, severity + 0.1)
    if len(item.body) > 400:
        severity = min(1.0, severity + 0.05)

    return ClassifiedEvent(
        event_type=event_type,
        region=region,
        severity=severity,
        assets_affected=assets,
        directional_bias=bias,
        tags=tags,
        rationale=",".join(rationale) or event_type,
        metadata={"headline": item.headline},
    )
