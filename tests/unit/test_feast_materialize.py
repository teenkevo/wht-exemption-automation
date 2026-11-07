"""Registration stores the view. Materialization makes one TIN readable online."""

import json
import os
from dataclasses import replace
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from data_engineering.config import ProjectConfig
from features.registry import check_online_store, materialize, open_store, register

PLAN = {
    "num_feats": ["log_total_customs_value", "has_total_customs_value", "business_age_years"],
    "cat_feats": ["REGION_NAME"],
    "passthrough": [],
}


def _snapshot(tmp_path):
    moment = datetime(2026, 11, 6, 9, 0, tzinfo=timezone.utc)
    features = tmp_path / "features"
    features.mkdir()
    pq.write_table(
        pa.table(
            {
                "TIN_NO": ["T1"],
                "label": pa.array([1], type=pa.int32()),
                "log_total_customs_value": pa.array([1.5], type=pa.float64()),
                "has_total_customs_value": pa.array([1], type=pa.int32()),
                "business_age_years": pa.array([10.0], type=pa.float64()),
                "REGION_NAME": ["NORTH"],
                "event_timestamp": pa.array([moment], type=pa.timestamp("us", tz="UTC")),
            }
        ),
        features / "part.parquet",
    )
    (tmp_path / "feature_plan.json").write_text(json.dumps(PLAN))
    return features


def _round_trip(config):
    registered = register(config)
    assert registered["feature_view"] == "taxpayer_features"
    assert "TIN_NO" not in registered["registered_features"]
    assert config.feast_registry.exists()

    materialize(config)
    row = open_store(config).get_online_features(
        features=[
            "taxpayer_features:business_age_years",
            "taxpayer_features:REGION_NAME",
            "taxpayer_features:label",
        ],
        entity_rows=[{"TIN_NO": "T1"}],
    ).to_dict()
    assert row["business_age_years"] == [10.0]
    assert row["REGION_NAME"] == ["NORTH"]
    assert row["label"] == [1]


def test_register_then_materialize_into_sqlite(tmp_path):
    features = _snapshot(tmp_path)
    config = replace(
        ProjectConfig.load(),
        features=features,
        feature_plan=tmp_path / "feature_plan.json",
        feast_registry=tmp_path / "registry.db",
        online_store="sqlite",
        online_connection=str(tmp_path / "online_store.db"),
    )
    _round_trip(config)


def test_register_then_materialize_into_redis(tmp_path):
    connection = os.getenv("TEST_REDIS", "localhost:6379")
    config = replace(
        ProjectConfig.load(),
        features=_snapshot(tmp_path),
        feature_plan=tmp_path / "feature_plan.json",
        feast_registry=tmp_path / "registry.db",
        online_store="redis",
        online_connection=connection,
    )
    try:
        check_online_store(config)
    except ConnectionError:
        pytest.skip("Redis is not running. Start it with: make redis-up")
    _round_trip(config)


def test_unreachable_redis_names_the_fix(tmp_path):
    config = replace(
        ProjectConfig.load(),
        features=_snapshot(tmp_path),
        feature_plan=tmp_path / "feature_plan.json",
        feast_registry=tmp_path / "registry.db",
        online_store="redis",
        online_connection="localhost:1",
    )
    with pytest.raises(ConnectionError, match="make redis-up"):
        register(config)
