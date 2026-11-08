"""Exemption cycles follow the financial year and only switch catalog rules on or off."""

from datetime import date

import pytest

from data_engineering.config import ProjectConfig
from rules_engine.cycles import DEFAULT_RULES, assessment_date, half_year, resolve_cycle

KNOWN = set(DEFAULT_RULES)


def _rules(*codes, off=()):
    return [{"code": code, "enabled": code not in off} for code in codes]


def _config(**cycles):
    return {"checklist": cycles}


H1 = {"rules": _rules(*DEFAULT_RULES)}
H2 = {"rules": _rules(*DEFAULT_RULES, off=("MIN_TAX_COLLECTED",))}


def test_project_config_picks_the_cycle_for_the_run_date():
    config = ProjectConfig.load()
    assert config.cycle.id == "wht_2026_27_h1"
    assert config.cycle.rule_codes == DEFAULT_RULES
    assert config.assessment_date == date(2026, 11, 7)


def test_financial_year_halves():
    assert half_year(date(2026, 7, 1)) == ("wht_2026_27_h1", date(2026, 7, 1), date(2026, 12, 31))
    assert half_year(date(2027, 3, 15)) == ("wht_2026_27_h2", date(2027, 1, 1), date(2027, 6, 30))


def test_run_date_selects_the_cycle_and_switches_rules_off():
    data = _config(wht_2026_27_h1=H1, wht_2026_27_h2=H2)
    cycle = resolve_cycle(data, known_rules=KNOWN, as_of=date(2027, 2, 1))
    assert cycle.id == "wht_2026_27_h2"
    assert (cycle.start, cycle.end) == (date(2027, 1, 1), date(2027, 6, 30))
    assert "MIN_TAX_COLLECTED" not in cycle.rule_codes


def test_a_cycle_without_config_uses_the_financial_half_name():
    cycle = resolve_cycle({}, known_rules=KNOWN, as_of=date(2026, 11, 7))
    assert cycle.id == "wht_2026_27_h1"
    assert (cycle.start, cycle.end) == (date(2026, 7, 1), date(2026, 12, 31))


def test_a_named_fallback_must_be_the_half_that_contains_the_date():
    with pytest.raises(ValueError, match="wht_2026_27_h2 should run from 2027-01-01 to 2027-06-30"):
        resolve_cycle({}, known_rules=KNOWN, as_of=date(2026, 11, 7), cycle_id="wht_2026_27_h2")


def test_missing_cycle_names_the_one_to_add():
    with pytest.raises(ValueError, match="Add wht_2026_27_h2 from 2027-01-01 to 2027-06-30"):
        resolve_cycle(_config(wht_2026_27_h1=H1), known_rules=KNOWN, as_of=date(2027, 2, 1))


@pytest.mark.parametrize(
    ("cycles", "message"),
    [
        ({"wht_2026_27_h1": {**H1, "start": "2026-07-01", "end": "2026-12-31"}}, "The name sets the window"),
        ({"wht_2026_28_h1": H1}, "should name financial year 2026_27"),
        ({"cycles": H1}, "Put cycle names directly under checklist"),
        ({"special": H1}, "must be named wht_YYYY_YY_h1 or wht_YYYY_YY_h2"),
        ({"wht_2026_27_h3": H1}, "must be named wht_YYYY_YY_h1 or wht_YYYY_YY_h2"),
        ({"wht_2026_27_h1": {**H1, "rules": _rules(*DEFAULT_RULES, off=DEFAULT_RULES)}}, "no enabled rules"),
        (
            {"wht_2026_27_h1": {**H1, "rules": [{"code": "MIN_TAX_COLLECTED", "expected_value": 1}]}},
            "Thresholds are set when the rule is registered.",
        ),
    ],
)
def test_cycle_config_is_checked(cycles, message):
    with pytest.raises(ValueError, match=message):
        resolve_cycle({"checklist": cycles}, known_rules=KNOWN, as_of=date(2026, 11, 7))


def test_named_cycle_is_assessed_inside_its_own_dates():
    cycle = resolve_cycle(_config(wht_2026_27_h1=H1, wht_2026_27_h2=H2), known_rules=KNOWN, cycle_id="wht_2026_27_h2")
    assert assessment_date(cycle, date(2026, 11, 7)) == date(2027, 1, 1)
    assert assessment_date(cycle, date(2027, 3, 1)) == date(2027, 3, 1)
