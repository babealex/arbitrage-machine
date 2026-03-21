from __future__ import annotations

from app.config import Settings, load_dotenv


def load_runtime_settings() -> Settings:
    load_dotenv()
    settings = Settings()
    settings.ensure_directories()
    return settings
