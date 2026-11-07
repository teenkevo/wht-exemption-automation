"""Feature contract for the taxpayer key."""

import pandas as pd
import pytest

from data_engineering.preprocess import preprocess
from features.compute_item_feats import compute_item_features
from features.compute_user_feats import LEAK_COLUMNS, compute_user_features


def test_user_features_require_the_taxpayer_key():
    with pytest.raises(ValueError, match="TIN_NO"):
        compute_user_features(pd.DataFrame({"other": [1]}))


def test_prepare_requires_the_taxpayer_key():
    with pytest.raises(ValueError, match="TIN_NO"):
        preprocess(pd.DataFrame({"other": [1]}))


def test_prepare_expects_spark():
    frame = pd.DataFrame({"TIN_NO": ["a"]})
    with pytest.raises(TypeError, match="Spark"):
        preprocess(frame)


def test_item_features_pass_through_for_the_single_entity():
    frame = pd.DataFrame({"TIN_NO": ["a"], "value": [1]})
    assert compute_item_features(frame).equals(frame)


def test_ranker_features_exclude_the_grant_inputs():
    assert {
        "TIN_NO",
        "wht_eligible",
        "REG_STATUS",
        "taxpayer_status",
        "total_outstanding_liability",
        "total_tax_collected",
        "total_returns_count",
    } <= LEAK_COLUMNS
