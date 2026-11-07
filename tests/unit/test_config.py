from data_engineering.config import ProjectConfig, SOURCE_NAMES


def test_local_mode_reads_the_joined_table(monkeypatch):
    monkeypatch.setenv("WHT_MODE", "local")
    config = ProjectConfig.load()
    assert config.project == "wht_exemption"
    assert config.entity_key == "TIN_NO"
    assert config.mode == "local"
    assert config.source_format == "parquet"
    assert config.lake_format == "delta"
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
    assert set(config.source_paths) == set(SOURCE_NAMES)
    assert config.source_paths["reg"].as_posix() == "/data/lakehouse/dm_modeled/dim_taxpayer_mst"
    assert config.source_paths["returns"].as_posix() == "/data/lakehouse/dm_modeled_ura/ft_it_returns_filled_ob"
    assert config.taxpayers.parts[-3:] == ("data", "delta", "wht_base_v1")


def test_mode_must_be_local_or_remote(monkeypatch):
    monkeypatch.setenv("WHT_MODE", "prod")
    try:
        ProjectConfig.load()
    except ValueError as exc:
        assert "local or remote" in str(exc)
    else:
        raise AssertionError("WHT_MODE=prod should be rejected")

