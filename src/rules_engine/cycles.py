"""Exemption cycles: which registered rules are switched on, and when.

Every cycle id is ``wht_YYYY_YY_h1`` or ``wht_YYYY_YY_h2``. ``YY`` is the next
year. ``h1`` runs 1 July to 31 December of ``YYYY``, and ``h2`` runs 1 January
to 30 June of the following year. Financial year 2026/27 is ``wht_2026_27_h1``
(1 July to 31 December 2026, Q1 and Q2) and ``wht_2026_27_h2`` (1 January to
30 June 2027, Q3 and Q4). The run date picks the cycle and is the assessment
date for the rules. ``WHT_AS_OF`` changes that date for a rerun. ``WHT_CYCLE``
names a cycle directly.

A cycle only switches rules on or off by code. A code applies once a version
is in force in the rules database. Thresholds are set when the rule is registered.
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime

DEFAULT_RULES = (
    "REGISTERED",
    "EXISTING_TAXPAYER",
    "NO_OUTSTANDING_LIABILITY",
    "RETURNS_FILED",
    "MIN_TAX_COLLECTED",
)


@dataclass(frozen=True)
class Cycle:
    id: str
    start: date
    end: date
    rule_codes: tuple[str, ...]


_CYCLE_NAME = re.compile(r"^wht_(\d{4})_(\d{2})_h([12])$")


def _financial_half(start_year: int, half: int) -> tuple[str, date, date]:
    name = f"wht_{start_year}_{(start_year + 1) % 100:02d}_h{half}"
    if half == 1:
        return name, date(start_year, 7, 1), date(start_year, 12, 31)
    return name, date(start_year + 1, 1, 1), date(start_year + 1, 6, 30)


def half_year(day: date) -> tuple[str, date, date]:
    """The cycle containing a date. July to December is h1, January to June is h2."""
    if day.month >= 7:
        return _financial_half(day.year, 1)
    return _financial_half(day.year - 1, 2)


def run_date() -> date:
    raw = os.getenv("WHT_AS_OF")
    if not raw:
        return date.today()
    try:
        return date.fromisoformat(raw.strip())
    except ValueError as exc:
        raise ValueError("WHT_AS_OF must be a date such as 2026-07-01") from exc


def _as_date(value: object, cycle_id: str, field: str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value.strip())
        except ValueError:
            pass
    raise ValueError(f"Checklist cycle {cycle_id} needs {field} as a date such as 2026-07-01")


def _require_cycle_name(cycle_id: str, start: date, end: date) -> None:
    """The id is ``wht_YYYY_YY_h1`` or ``wht_YYYY_YY_h2``, and the dates are that half."""
    named = _CYCLE_NAME.match(str(cycle_id))
    if named is None:
        raise ValueError(
            f"Checklist cycle {cycle_id} must be named wht_YYYY_YY_h1 or wht_YYYY_YY_h2, "
            "for example wht_2026_27_h1"
        )
    start_year = int(named.group(1))
    if int(named.group(2)) != (start_year + 1) % 100:
        raise ValueError(
            f"Checklist cycle {cycle_id} should name financial year "
            f"{start_year}_{(start_year + 1) % 100:02d}"
        )
    expected_name, expected_start, expected_end = _financial_half(start_year, int(named.group(3)))
    if str(cycle_id) != expected_name or (start, end) != (expected_start, expected_end):
        raise ValueError(f"Checklist cycle {cycle_id} should run from {expected_start} to {expected_end}")


def _cycle_dates(cycles: Mapping) -> dict[str, tuple[date, date]]:
    spans: dict[str, tuple[date, date]] = {}
    for cycle_id, body in cycles.items():
        body = body or {}
        start = _as_date(body.get("start"), cycle_id, "start")
        end = _as_date(body.get("end"), cycle_id, "end")
        if end < start:
            raise ValueError(f"Checklist cycle {cycle_id} ends before it starts")
        _require_cycle_name(str(cycle_id), start, end)
        spans[str(cycle_id)] = (start, end)
    ordered = sorted(spans.items(), key=lambda item: item[1][0])
    for (first, (_start, first_end)), (second, (second_start, _end)) in zip(ordered, ordered[1:]):
        if second_start <= first_end:
            raise ValueError(f"Checklist cycles {first} and {second} overlap")
    return spans


def _rule_codes(selected: str, entries: object, known: set[str]) -> tuple[str, ...]:
    # ``known`` is the codes registered so far. A cycle may name a code before
    # that version exists; it applies once the version is in force.
    del known
    if not isinstance(entries, list) or not entries:
        raise ValueError(f"Checklist cycle {selected} needs a list of rules")
    codes: list[str] = []
    seen: set[str] = set()
    for raw in entries:
        if not isinstance(raw, dict) or not raw.get("code"):
            raise ValueError(f"Checklist cycle {selected} has a rule without a code")
        code = str(raw["code"])
        if code in seen:
            raise ValueError(f"Checklist cycle {selected} repeats rule {code}")
        seen.add(code)
        extra = sorted(str(key) for key in raw if key not in {"code", "enabled"})
        if extra:
            raise ValueError(
                f"Checklist cycle {selected} rule {code} sets {', '.join(extra)}. "
                "Cycles only switch rules on or off. Thresholds are set when the rule is registered."
            )
        if bool(raw.get("enabled", True)):
            codes.append(code)
    if not codes:
        raise ValueError(f"Checklist cycle {selected} has no enabled rules")
    return tuple(codes)


def resolve_cycle(
    data: Mapping,
    *,
    known_rules: set[str],
    cycle_id: str | None = None,
    as_of: date | None = None,
) -> Cycle:
    """Pick the cycle for the run date, or the one named in ``WHT_CYCLE``."""
    configured = data.get("checklist") or {}
    cycles = configured.get("cycles") or {}
    day = as_of or run_date()
    named = cycle_id or os.getenv("WHT_CYCLE")
    if not cycles:
        name, start, end = half_year(day)
        chosen = str(named or name)
        _require_cycle_name(chosen, start, end)
        return Cycle(chosen, start, end, DEFAULT_RULES)
    spans = _cycle_dates(cycles)
    if named:
        selected = str(named)
        if selected not in cycles:
            raise ValueError(f"Unknown checklist cycle {selected!r}. Known cycles: {', '.join(sorted(cycles))}")
    else:
        covering = [name for name, (start, end) in spans.items() if start <= day <= end]
        if not covering:
            name, start, end = half_year(day)
            raise ValueError(f"No checklist cycle covers {day}. Add {name} from {start} to {end} in config/config.yaml")
        selected = covering[0]
    start, end = spans[selected]
    codes = _rule_codes(selected, (cycles[selected] or {}).get("rules"), known_rules)
    return Cycle(selected, start, end, codes)


def assessment_date(cycle: Cycle, day: date | None = None) -> date:
    """The run date, held inside the cycle so a named cycle is judged by its own rules."""
    day = day or run_date()
    return min(max(day, cycle.start), cycle.end)
