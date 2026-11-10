import pytest

from data_engineering.config import LAKE_PREFIX, ProjectConfig, SOURCE_NAMES, lake_relative


def test_local_mode_reads_the_joined_table(monkeypatch):
    monkeypatch.setenv("WHT_MODE", "local")
    config = ProjectConfig.load()
    assert config.project == "wht_exemption"
    assert config.entity_key == "TIN_NO"
    assert config.mode == "local"
    assert config.source_format == "parquet"
    assert config.lake_format == "delta"
    assert config.lake_access == "file"
    assert config.serving_model == "random_forest"
    assert config.dedupe_sector is True
    assert config.source_paths == {}
    assert config.taxpayers.name == "wht_base_v1"
    assert config.taxpayers.parts[-3:] == ("data", "delta", "wht_base_v1")
    assert config.grants.name == "wht_grants.parquet"


def test_remote_mode_reads_the_lake_and_keeps_the_saved_table(monkeypatch):
    monkeypatch.setenv("WHT_MODE", "remote")
    config = ProjectConfig.load()
    assert config.mode == "remote"
    assert config.source_format == "parquet"
    assert config.lake_format == "delta"
    assert config.lake_access == "drill"
    assert config.drill_url.startswith("jdbc:drill:drillbit=uradfnode3.ura.go.ug:31010")
    assert config.drill_storage == "dfs.root"
    assert config.drill_password == ""
    assert set(config.source_paths) == set(SOURCE_NAMES)
    assert config.source_paths["reg"].as_posix() == "/data/lakehouse/dm_modeled/dim_taxpayer_mst"
    assert config.source_paths["returns"].as_posix() == "/data/lakehouse/dm_modeled_ura/ft_it_returns_filled_ob"
    assert config.taxpayers.parts[-3:] == ("data", "delta", "wht_base_v1")


def test_lake_root_retargets_the_remote_sources(monkeypatch):
    monkeypatch.setenv("WHT_MODE", "remote")
    monkeypatch.setenv("WHT_LAKE_ROOT", "mock_lake")
    config = ProjectConfig.load()
    reg = config.source_paths["reg"]
    assert reg.parts[-3:] == ("mock_lake", "dm_modeled", "dim_taxpayer_mst")
    assert reg.is_absolute()
    returns = config.source_paths["returns"]
    assert returns.parts[-3:] == ("mock_lake", "dm_modeled_ura", "ft_it_returns_filled_ob")
    assert set(config.source_paths) == set(SOURCE_NAMES)
    assert config.lake_access == "file"


def test_an_absolute_lake_root_keeps_the_remote_relative_paths(monkeypatch, tmp_path):
    monkeypatch.setenv("WHT_MODE", "remote")
    monkeypatch.setenv("WHT_LAKE_ROOT", str(tmp_path))
    config = ProjectConfig.load()
    assert config.source_paths["payments"] == tmp_path / "dm_modeled" / "fact_payments"
    assert config.source_paths["customs"] == tmp_path / "dm_modeled_ura" / "ft_cstm_decl_hdr"


def test_lake_relative_rejects_a_path_outside_the_lake():
    with pytest.raises(ValueError, match="outside"):
        lake_relative("/tmp/fact_payments")
    assert lake_relative(f"{LAKE_PREFIX}/dm_modeled/fact_ledger") == "dm_modeled/fact_ledger"


def test_lake_access_can_stay_on_files(monkeypatch):
    monkeypatch.setenv("WHT_MODE", "remote")
    monkeypatch.setenv("WHT_LAKE_ACCESS", "file")
    config = ProjectConfig.load()
    assert config.lake_access == "file"
    assert config.source_paths["reg"].as_posix() == "/data/lakehouse/dm_modeled/dim_taxpayer_mst"


def test_drill_env_replaces_the_connection_file(monkeypatch, tmp_path):
    jar = tmp_path / "drill-jdbc-all-1.22.0.jar"
    jar.write_bytes(b"")
    monkeypatch.setenv("WHT_MODE", "remote")
    monkeypatch.setenv("DRILL_URL", "jdbc:drill:drillbit=drill.internal:31010;auth=plain")
    monkeypatch.setenv("DRILL_USER", "reader")
    monkeypatch.setenv("DRILL_PASSWORD", "secret")
    monkeypatch.setenv("DRILL_JDBC_JAR", str(jar))
    config = ProjectConfig.load()
    assert config.drill_url.endswith("drill.internal:31010;auth=plain")
    assert config.drill_user == "reader"
    assert config.drill_password == "secret"
    assert config.drill_jar == jar


def test_mode_must_be_local_or_remote(monkeypatch):
    monkeypatch.setenv("WHT_MODE", "prod")
    try:
        ProjectConfig.load()
    except ValueError as exc:
        assert "local or remote" in str(exc)
    else:
        raise AssertionError("WHT_MODE=prod should be rejected")

