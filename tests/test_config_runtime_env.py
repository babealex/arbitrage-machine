from app.config import Settings


def test_settings_read_environment_at_instantiation(monkeypatch) -> None:
    monkeypatch.setenv("REPORT_PATH", "./data/runtime_report.json")
    monkeypatch.setenv("STRATEGY_TRANSPARENCY_PACK_PATH", "./data/transparency_pack.json")
    monkeypatch.setenv("APP_MODE", "safe")

    settings = Settings()

    assert settings.report_path.as_posix().endswith("runtime_report.json")
    assert settings.strategy_transparency_pack_path.as_posix().endswith("transparency_pack.json")
    assert settings.app_mode == "safe"
