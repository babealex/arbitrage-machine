from app.persistence.schema.allocation import ALLOCATION_SCHEMA
from app.persistence.schema.allocation_outcomes import ALLOCATION_OUTCOMES_SCHEMA
from app.persistence.schema.belief import BELIEF_SCHEMA
from app.persistence.schema.convexity import CONVEXITY_SCHEMA
from app.persistence.schema.core import CORE_SCHEMA
from app.persistence.schema.event import EVENT_SCHEMA
from app.persistence.schema.kalshi import KALSHI_SCHEMA
from app.persistence.schema.options import OPTIONS_SCHEMA
from app.persistence.schema.portfolio_cycle import PORTFOLIO_CYCLE_SCHEMA
from app.persistence.schema.trend_broker import TREND_BROKER_SCHEMA
from app.persistence.schema.trend import TREND_SCHEMA


def schema_sql() -> str:
    return "\n".join(
        [
            CORE_SCHEMA.strip(),
            CONVEXITY_SCHEMA.strip(),
            ALLOCATION_SCHEMA.strip(),
            ALLOCATION_OUTCOMES_SCHEMA.strip(),
            KALSHI_SCHEMA.strip(),
            TREND_SCHEMA.strip(),
            TREND_BROKER_SCHEMA.strip(),
            PORTFOLIO_CYCLE_SCHEMA.strip(),
            OPTIONS_SCHEMA.strip(),
            BELIEF_SCHEMA.strip(),
            EVENT_SCHEMA.strip(),
        ]
    )
