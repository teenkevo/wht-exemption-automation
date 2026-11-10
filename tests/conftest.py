import os

import pytest

# Rules come from the in-memory catalog unless a test opens the database itself.
os.environ["RULES_STORE"] = "memory"


@pytest.fixture(autouse=True)
def pipeline_env(monkeypatch):
    monkeypatch.setenv("WHT_MODE", "local")
    monkeypatch.setenv("WHT_AS_OF", "2026-11-07")
    monkeypatch.delenv("WHT_CYCLE", raising=False)
    monkeypatch.delenv("WHT_LAKE_ROOT", raising=False)
    monkeypatch.delenv("WHT_LAKE_ACCESS", raising=False)
    monkeypatch.delenv("DRILL_URL", raising=False)
    monkeypatch.delenv("DRILL_USER", raising=False)
    monkeypatch.delenv("DRILL_PASSWORD", raising=False)
    monkeypatch.delenv("DRILL_JDBC_JAR", raising=False)
    monkeypatch.setenv("RULES_STORE", "memory")


@pytest.fixture(scope="session")
def spark():
    try:
        from data_engineering.spark import spark_session

        try:
            import delta  # noqa: F401

            delta_enabled = True
        except ImportError:
            delta_enabled = False
        session = spark_session("pipeline-tests", master="local[1]", delta=delta_enabled)
    except Exception as exc:
        pytest.skip(f"Spark is not available: {exc}")
    yield session
    session.stop()
