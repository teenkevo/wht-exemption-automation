"""Feast repo files declare the taxpayer view."""

from pathlib import Path

import yaml

from feature_store.features import feature_names, feature_view_for_plan, taxpayer

ROOT = Path(__file__).resolve().parents[2]


def test_feature_store_yaml_names_the_project():
    loaded = yaml.safe_load((ROOT / "feature_store" / "feature_store.yaml").read_text())
    assert loaded["project"] == "wht_exemption"
    assert loaded["provider"] == "local"
    assert loaded["online_store"]["type"] == "redis"


def test_feature_view_uses_the_taxpayer_key(tmp_path):
    plan = {
        "num_feats": ["log_total_customs_value", "has_total_customs_value", "business_age_years"],
        "cat_feats": ["REGION_NAME"],
        "passthrough": [],
    }
    view = feature_view_for_plan(plan, tmp_path / "features")
    assert taxpayer.join_key == "TIN_NO"
    assert view.name == "taxpayer_features"
    assert view.online is True
    assert feature_names(plan) == [
        "label",
        "log_total_customs_value",
        "has_total_customs_value",
        "business_age_years",
        "REGION_NAME",
    ]
    assert set(feature_names(plan)) <= {field.name for field in view.schema}
