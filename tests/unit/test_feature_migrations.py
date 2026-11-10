"""Feature SQL migrations: the latest expression for a name is the active one."""

import pytest

from rules_engine.feature_computations import FEATURES, FeatureStatus
from rules_engine.feature_migrations import load_feature_registry


def _file(directory, name: str, *blocks: str) -> None:
    directory.joinpath(name).write_text("\n\n".join(blocks) + "\n")


def _block(feature: str, query: str, **headers: str) -> str:
    fields = {
        "description": "Total tax paid",
        "dtype": "number",
        "source": "payments",
        "missing": "0, no payments on record",
        **headers,
    }
    lines = [f"-- feature: {feature}"]
    lines.extend(f"-- {key}: {value}" for key, value in fields.items())
    lines.append(query)
    return "\n".join(lines)


_TAX = """SELECT
    TIN_NO,
    cast(sum(TOT_PAYMENT_AMT) as double) AS tax_collected
FROM lakehouse.payments
GROUP BY TIN_NO"""


def test_baseline_features_start_at_version_one():
    feature = FEATURES.require("tax_collected")
    assert feature.version == 1
    assert feature.status == FeatureStatus.ACTIVE
    assert feature.key == "tax_collected:v1"
    assert feature.sql == _TAX
    assert feature.source_table == "payments"
    assert len(FEATURES.names()) == 12
    assert FEATURES.history("tax_collected") == (feature,)


def test_a_later_migration_supersedes_the_feature(tmp_path):
    _file(tmp_path, "V001__baseline.sql", _block("tax_collected", _TAX))
    revised = _TAX.replace(
        "cast(sum(TOT_PAYMENT_AMT) as double)",
        "cast(sum(TOT_PAYMENT_AMT) - sum(reversed_tax) as double)",
    )
    _file(
        tmp_path,
        "V002__net_of_reversals.sql",
        _block("tax_collected", revised, description="Total tax paid, excluding reversed payments"),
    )
    features = load_feature_registry(tmp_path)
    active = features.require("tax_collected")
    history = features.history("tax_collected")
    assert active.version == 2
    assert active.key == "tax_collected:v2"
    assert "reversed_tax" in active.sql
    assert history[0].status == FeatureStatus.DEPRECATED
    assert history[0].version == 1
    assert history[1] == active


@pytest.mark.parametrize(
    ("files", "message"),
    [
        ({"V001__a.sql": "", "V003__c.sql": ""}, "no gaps"),
        ({"draft.sql": ""}, "V001__short_name.sql"),
        ({"V001__a.sql": "-- description: missing the feature line\ncast(1 as double)\n"}, "needs a feature block"),
    ],
)
def test_migration_files_are_checked(tmp_path, files, message):
    for name, text in files.items():
        (tmp_path / name).write_text(text)
    with pytest.raises(ValueError, match=message):
        load_feature_registry(tmp_path)


def test_a_feature_block_must_be_one_expression(tmp_path):
    twice = tmp_path / "twice"
    script = tmp_path / "script"
    twice.mkdir()
    script.mkdir()
    _file(twice, "V001__twice.sql", _block("tax_collected", _TAX) + "\n" + _block("tax_collected", _TAX))
    with pytest.raises(ValueError, match="defines tax_collected more than once"):
        load_feature_registry(twice)

    _file(script, "V001__script.sql", _block("tax_collected", "select 1; select 2"))
    with pytest.raises(ValueError, match="one SELECT"):
        load_feature_registry(script)
