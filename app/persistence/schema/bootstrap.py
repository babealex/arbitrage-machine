from app.persistence.schema.belief import BELIEF_SCHEMA
from app.persistence.schema.core import CORE_SCHEMA
from app.persistence.schema.event import EVENT_SCHEMA
from app.persistence.schema.kalshi import KALSHI_SCHEMA


def schema_sql() -> str:
    return "\n".join(
        [
            CORE_SCHEMA.strip(),
            KALSHI_SCHEMA.strip(),
            BELIEF_SCHEMA.strip(),
            EVENT_SCHEMA.strip(),
        ]
    )
