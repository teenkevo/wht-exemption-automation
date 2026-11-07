import os

import pytest

# Rules come from the in-memory catalog unless a test opens the database itself.
os.environ["RULES_STORE"] = "memory"


@pytest.fixture(autouse=True)
def pipeline_env(monkeypatch):
    monkeypatch.setenv("WHT_MODE", "local")
    monkeypatch.setenv("WHT_AS_OF", "2026-11-07")
    monkeypatch.delenv("WHT_CYCLE", raising=False)
    monkeypatch.setenv("RULES_STORE", "memory")


@pytest.fixture(scope="session")
def spark():
    try:
        from data_engineering.spark import spark_session

        session = spark_session("pipeline-tests", master="local[1]")
    except Exception as exc:
        pytest.skip(f"Spark is not available: {exc}")
    yield session
    session.stop()
