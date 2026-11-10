import pytest

from data_engineering.drill import drill_dbtable, open_drill


def test_drill_query_quotes_the_lake_path_and_columns():
    query = drill_dbtable(
        "dfs.root",
        "/data/lakehouse/dm_modeled/dim_taxpayer_mst",
        ("TIN_NO", "taxpayer_status"),
    )
    assert query == (
        "(SELECT `TIN_NO`, `taxpayer_status` "
        "FROM dfs.root.`data/lakehouse/dm_modeled/dim_taxpayer_mst`) AS src"
    )


def test_drill_query_can_limit_a_connection_check():
    query = drill_dbtable("dfs.root", "/data/lakehouse/dm_modeled/fact_ledger", ("TPIN",), limit=1)
    assert query.endswith("LIMIT 1) AS src")


def test_drill_query_rejects_an_unsafe_path():
    with pytest.raises(ValueError, match="lake path"):
        drill_dbtable("dfs.root", "/data/lakehouse/../secret", ("TIN_NO",))


def test_open_drill_requires_a_password_and_the_driver_jar(monkeypatch, tmp_path):
    monkeypatch.setenv("WHT_MODE", "remote")
    monkeypatch.setenv("DRILL_USER", "reader")
    monkeypatch.delenv("DRILL_PASSWORD", raising=False)
    from data_engineering.config import ProjectConfig

    config = ProjectConfig.load()
    with pytest.raises(ValueError, match="DRILL_PASSWORD"):
        open_drill(config)

    jar = tmp_path / "drill-jdbc-all-1.22.0.jar"
    jar.write_bytes(b"")
    monkeypatch.setenv("DRILL_USER", "reader")
    monkeypatch.setenv("DRILL_PASSWORD", "secret")
    monkeypatch.setenv("DRILL_JDBC_JAR", str(jar))
    connection = open_drill(ProjectConfig.load())
    assert connection.user == "reader"
    assert connection.jar == jar
    assert connection.storage == "dfs.root"
