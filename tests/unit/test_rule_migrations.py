"""Migration files are ordered, checksummed, and rejected before they touch a database."""

from datetime import date
from pathlib import Path

import pytest

from rules_engine.migrations import load_migrations, migrations_dir, prepare

SHIPPED = migrations_dir()


def _file(directory: Path, name: str, body: str) -> None:
    directory.joinpath(name).write_text(body)


def _rule(code: str = "MIN_SALES", feature: str = "efris_sales_total") -> str:
    return f"""
description: One rule
changes:
  - rule_code: {code}
    title: Sales
    description: Sales at least 10M
    feature_name: {feature}
    operator: gte
    expected_value: 10000000
    effective_from: 2026-07-01
    reason: Baseline
"""


def test_shipped_baseline_registers_the_five_rules():
    files = load_migrations(SHIPPED)
    assert [migration.version for migration in files] == [1]
    assert [change.rule_code for change in files[0].changes] == [
        "REGISTERED",
        "EXISTING_TAXPAYER",
        "NO_OUTSTANDING_LIABILITY",
        "RETURNS_FILED",
        "MIN_TAX_COLLECTED",
    ]
    assert files[0].changes[0].effective_from == date(2026, 7, 1)


def test_versions_must_be_contiguous(tmp_path):
    _file(tmp_path, "V001__first.yaml", _rule())
    _file(tmp_path, "V003__third.yaml", _rule("MIN_TAX_COLLECTED", "tax_collected"))
    with pytest.raises(ValueError, match="Missing V2"):
        load_migrations(tmp_path)


def test_an_unknown_feature_is_rejected(tmp_path):
    _file(tmp_path, "V001__missing.yaml", _rule(feature="not_a_feature"))
    with pytest.raises(ValueError, match="unknown feature"):
        load_migrations(tmp_path)


def test_a_database_with_rules_and_no_history_is_not_replayed(tmp_path):
    _file(tmp_path, "V001__first.yaml", _rule())
    files = load_migrations(tmp_path)
    with pytest.raises(ValueError, match="--baseline"):
        prepare(files, [], has_rules=True, baseline=None)


def test_an_edited_file_is_rejected(tmp_path):
    _file(tmp_path, "V001__first.yaml", _rule())
    files = load_migrations(tmp_path)
    applied = [{"version": 1, "checksum": "not-the-file"}]
    with pytest.raises(ValueError, match="edited after it was applied"):
        prepare(files, applied, has_rules=True, baseline=None)


def test_baseline_records_files_without_running_later_ones(tmp_path):
    _file(tmp_path, "V001__first.yaml", _rule())
    _file(tmp_path, "V002__second.yaml", _rule("MIN_TAX_COLLECTED", "tax_collected"))
    files = load_migrations(tmp_path)
    plan = prepare(files, [], has_rules=True, baseline=1)
    assert [migration.version for migration in plan.baseline] == [1]
    assert plan.pending == ()
