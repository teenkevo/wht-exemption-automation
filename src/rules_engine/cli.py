"""Manage checklist rules in the rules database.

    python -m rules_engine.cli init                        create the schema; leave it empty
    python -m rules_engine.cli list [--on 2026-11-07]      rules in force on a date
    python -m rules_engine.cli history CODE                every version of one rule
    python -m rules_engine.cli audit [CODE] [--limit 50]   who changed what, and why
    python -m rules_engine.cli runs [--limit 10]           grant runs and the versions they used
    python -m rules_engine.cli register FILE.yaml [--rm]   new DRAFT version from a rule file
    python -m rules_engine.cli approve CODE VERSION        sign off a DRAFT (not by its author)
    python -m rules_engine.cli activate CODE VERSION --from 2027-01-01
    python -m rules_engine.cli retire CODE --from 2027-07-01

``--actor`` names the person (default: ``WHT_ACTOR`` or the login name).
``--reason`` is stored in the audit trail.
"""

from __future__ import annotations

import argparse
import getpass
import json
import os
import sys
from datetime import date
from pathlib import Path

import yaml

from rules_engine.feature_computations import FEATURES
from rules_engine.catalog import rules_store_settings, seed
from rules_engine.db import PostgresRuleRegistry, RulesDatabaseError, redact
from rules_engine.rule_models import RuleDefinition


def _registry() -> PostgresRuleRegistry:
    kind, dsn = rules_store_settings()
    if kind != "postgres":
        raise SystemExit("The rules CLI manages the PostgreSQL rules store. Set rules_store.type: postgres")
    return PostgresRuleRegistry(dsn, FEATURES)


def _value(value) -> str:
    return json.dumps(value) if not isinstance(value, str) else value


def _print_rules(rules) -> None:
    for rule in rules:
        window = f"{rule.effective_from or '-'} to {rule.effective_to or 'open'}"
        print(
            f"{rule.key:<30} {rule.status.value:<10} {window:<26} "
            f"{rule.feature_name} {rule.operator.value} {_value(rule.expected_value)}"
            f"{'' if rule.mandatory else '  (optional)'}"
        )


def _init(registry: PostgresRuleRegistry, _args) -> None:
    registry.init_schema()
    if registry.is_empty():
        seed(registry)
    if registry.is_empty():
        print(f"Schema ready at {redact(registry.dsn)}; no rules registered")
    else:
        print(f"Schema ready at {redact(registry.dsn)}; existing rules kept")
    _print_rules(registry.get_active_rules(date.today()))


def _list(registry, args) -> None:
    day = args.on or date.today()
    rules = registry.get_active_rules(day)
    print(f"Rules in force on {day}: {len(rules)}")
    _print_rules(rules)


def _history(registry, args) -> None:
    rules = registry.history(args.code)
    if not rules:
        raise SystemExit(f"{args.code} has no versions")
    for rule in rules:
        _print_rules([rule])
        print(f"{'':<30} by {rule.created_by}, approved by {rule.approved_by or '-'}: {rule.description}")


def _audit(registry, args) -> None:
    for event in registry.audit_log(args.code, limit=args.limit):
        action = getattr(event.action, "value", event.action)
        key = f"{event.rule_code}:v{event.version}"
        print(f"{event.at:%Y-%m-%d %H:%M:%S} {action:<11} {key:<30} {event.actor:<20} {event.detail}")


def _runs(registry, args) -> None:
    for run in registry.runs(args.limit):
        print(
            f"#{run['id']} {run['run_at']:%Y-%m-%d %H:%M} {run['actor']} {run['cycle']} assessed {run['assessment_date']}: "
            f"{run['taxpayer_count']:,} taxpayers, {run['eligible']:,} eligible, "
            f"{run['needs_review']:,} review; {', '.join(run['rules_applied'])}"
        )


def _register(registry, args) -> None:
    path = Path(args.file)
    raw = yaml.safe_load(path.read_text())
    if not isinstance(raw, dict):
        raise SystemExit(f"{args.file} must hold one rule as a mapping")
    blocked = sorted(set(raw) & {"version", "status", "effective_from", "effective_to", "created_by", "approved_by"})
    if blocked:
        raise SystemExit(f"{args.file} sets {', '.join(blocked)}; the registry assigns those")
    draft = registry.register(RuleDefinition(**raw), actor=args.actor, reason=args.reason)
    if args.rm:
        path.unlink()
        print(f"Removed {path}")
    print(f"Registered {draft.key} as DRAFT. Next: another person runs approve {draft.rule_code} {draft.version}")


def _approve(registry, args) -> None:
    rule = registry.approve(args.code, args.version, actor=args.actor, reason=args.reason)
    print(f"Approved {rule.key}. Next: activate {rule.rule_code} {rule.version} --from YYYY-MM-DD")


def _activate(registry, args) -> None:
    rule = registry.activate(args.code, args.version, args.effective_from, actor=args.actor, reason=args.reason)
    print(f"{rule.key} is in force from {rule.effective_from}")


def _retire(registry, args) -> None:
    rule = registry.retire(args.code, args.effective_from, actor=args.actor, reason=args.reason)
    print(f"{rule.key} stops applying on {rule.effective_to}")


def parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--actor", default=os.getenv("WHT_ACTOR") or getpass.getuser())
    common.add_argument("--reason", default="")
    top = argparse.ArgumentParser(prog="rules", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = top.add_subparsers(dest="command", required=True)

    commands.add_parser("init", parents=[common]).set_defaults(run=_init)
    listing = commands.add_parser("list", parents=[common])
    listing.add_argument("--on", type=date.fromisoformat)
    listing.set_defaults(run=_list)
    history = commands.add_parser("history", parents=[common])
    history.add_argument("code")
    history.set_defaults(run=_history)
    audit = commands.add_parser("audit", parents=[common])
    audit.add_argument("code", nargs="?")
    audit.add_argument("--limit", type=int, default=50)
    audit.set_defaults(run=_audit)
    runs = commands.add_parser("runs", parents=[common])
    runs.add_argument("--limit", type=int, default=10)
    runs.set_defaults(run=_runs)
    register = commands.add_parser("register", parents=[common])
    register.add_argument("file")
    register.add_argument("--rm", action="store_true", help="Delete the rule file after the draft is stored")
    register.set_defaults(run=_register)
    approve = commands.add_parser("approve", parents=[common])
    approve.add_argument("code")
    approve.add_argument("version", type=int)
    approve.set_defaults(run=_approve)
    activate = commands.add_parser("activate", parents=[common])
    activate.add_argument("code")
    activate.add_argument("version", type=int)
    activate.add_argument("--from", dest="effective_from", type=date.fromisoformat, required=True)
    activate.set_defaults(run=_activate)
    retire = commands.add_parser("retire", parents=[common])
    retire.add_argument("code")
    retire.add_argument("--from", dest="effective_from", type=date.fromisoformat, required=True)
    retire.set_defaults(run=_retire)
    return top


def main(argv: list[str] | None = None) -> None:
    args = parser().parse_args(argv)
    registry = _registry()
    try:
        if args.command != "init":
            registry.check()
        args.run(registry, args)
    except (RulesDatabaseError, ValueError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
    except Exception as exc:
        import psycopg

        if isinstance(exc, psycopg.Error):
            message = exc.diag.message_primary if getattr(exc, "diag", None) else str(exc)
            print(f"Database refused the change: {message}", file=sys.stderr)
            raise SystemExit(1) from exc
        raise
    finally:
        registry.close()


if __name__ == "__main__":
    main()
