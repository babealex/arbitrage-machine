from app.config import Settings


def test_settings_read_environment_at_instantiation(monkeypatch) -> None:
    monkeypatch.setenv("REPORT_PATH", "./data/runtime_report.json")
    monkeypatch.setenv("APP_MODE", "safe")

    settings = Settings()

    assert settings.report_path.as_posix().endswith("runtime_report.json")
    assert settings.app_mode == "safe"
