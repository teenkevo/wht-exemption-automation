"""Manage checklist rules in the rules database.

    python -m rules_engine.cli init                        create the schema; leave it empty
    python -m rules_engine.cli list [--on 2026-11-07]      rules in force on a date
    python -m rules_engine.cli history CODE                every version of one rule
    python -m rules_engine.cli audit [CODE] [--limit 50]   who changed what, and why
    python -m rules_engine.cli runs [--limit 10]           grant runs and the versions they used
    python -m rules_engine.cli register FILE.yaml [--rm]   new DRAFT version from a rule file
    python -m rules_engine.cli activate CODE VERSION --from 2027-01-01   put a DRAFT in force
    python -m rules_engine.cli retire CODE --from 2027-07-01
    python -m rules_engine.cli migrations                  which migration files are applied
    python -m rules_engine.cli migrate                     apply pending files, in order
    python -m rules_engine.cli migrate --baseline 1        record files through V1 without running them

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
from rules_engine.migrations import load_migrations, migrations_dir, prepare
from rules_engine.rule_models import RuleDefinition
from termstyle import heading, lines, note, paint


def _registry() -> PostgresRuleRegistry:
    kind, dsn = rules_store_settings()
    if kind != "postgres":
        raise SystemExit("The rules CLI manages the PostgreSQL rules store. Set rules_store.type: postgres")
    return PostgresRuleRegistry(dsn, FEATURES)


def _value(value) -> str:
    return json.dumps(value) if not isinstance(value, str) else value


_STATUS_COLOR = {
    "ACTIVE": "green",
    "DRAFT": "yellow",
    "APPROVED": "cyan",
    "SUPERSEDED": "dim",
    "RETIRED": "red",
}


def _print_rules(rules) -> None:
    for rule in rules:
        window = f"{rule.effective_from or '-'} to {rule.effective_to or 'open'}"
        status = rule.status.value
        optional = "" if rule.mandatory else "  optional"
        key = paint(f"{rule.key:<28}", "cyan")
        status_cell = paint(f"{status:<10}", _STATUS_COLOR.get(status, ""))
        window_cell = paint(f"{window:<24}", "dim")
        print(f"  {key}  {status_cell}  {window_cell}  {rule.feature_name} {rule.operator.value} {_value(rule.expected_value)}{optional}")


def _init(registry: PostgresRuleRegistry, _args) -> None:
    registry.init_schema()
    if registry.is_empty():
        seed(registry)
    heading("Rules database")
    if registry.is_empty():
        note(f"Schema ready at {redact(registry.dsn)}")
        note("No rules registered")
        return
    note(f"Schema ready at {redact(registry.dsn)}")
    print()
    _print_rules(registry.get_active_rules(date.today()))


def _list(registry, args) -> None:
    day = args.on or date.today()
    rules = registry.get_active_rules(day)
    heading(f"Rules in force on {day}")
    note(f"{len(rules)} rule{'s' if len(rules) != 1 else ''}")
    print()
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
    print(f"Registered {draft.key} as DRAFT. Next: activate {draft.rule_code} {draft.version} --from YYYY-MM-DD")


def _approve(registry, args) -> None:
    rule = registry.approve(args.code, args.version, actor=args.actor, reason=args.reason)
    print(f"Approved {rule.key}. Next: activate {rule.rule_code} {rule.version} --from YYYY-MM-DD")


def _activate(registry, args) -> None:
    rule = registry.activate(args.code, args.version, args.effective_from, actor=args.actor, reason=args.reason)
    print(f"{rule.key} is in force from {rule.effective_from}")


def _migration_files(registry):
    registry.init_schema()
    return load_migrations(migrations_dir(), registry.features), registry.applied_migrations()


def _migrations(registry, _args) -> None:
    files, applied = _migration_files(registry)
    recorded = {int(row["version"]): row for row in applied}
    heading("Migrations")
    note(str(migrations_dir()))
    print()
    for migration in files:
        row = recorded.get(migration.version)
        if row is None:
            state, color = "pending", "yellow"
        elif row["checksum"] != migration.checksum:
            state, color = "edited after apply", "red"
        else:
            state, color = f"applied {row['applied_at']:%Y-%m-%d %H:%M} by {row['applied_by']}", "green"
        print(f"  {paint(f'V{migration.version:03d}', 'cyan')}  {migration.description:<42} {paint(state, color)}")


def _migrate(registry, args) -> None:
    files, applied = _migration_files(registry)
    plan = prepare(files, applied, has_rules=not registry.is_empty(), baseline=args.baseline)
    if plan.baseline:
        heading("Baseline")
        for migration in plan.baseline:
            registry.record_baseline(migration, actor=args.actor)
            print(f"  {paint(f'V{migration.version:03d}', 'cyan')}  {migration.description}")
        leftover = [migration for migration in files if migration.version > args.baseline]
        if leftover:
            print()
            note(f"{len(leftover)} still pending. Run make rules-migrate to apply them.")
        return
    if not plan.pending:
        heading("Migrations")
        note("No pending migrations")
        return
    for migration in plan.pending:
        rules = registry.apply_migration(migration, actor=args.actor)
        heading(f"Applied V{migration.version:03d}")
        note(migration.description)
        print()
        lines(
            [
                f"{rule.key}  {rule.status.value}  from "
                f"{rule.effective_to if rule.effective_to and rule.status.value == 'RETIRED' else rule.effective_from}"
                for rule in rules
            ],
            color="cyan",
        )


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
    commands.add_parser("migrations", parents=[common]).set_defaults(run=_migrations)
    migrate = commands.add_parser("migrate", parents=[common])
    migrate.add_argument("--baseline", type=int, help="Record files through this version without running them")
    migrate.set_defaults(run=_migrate)
    return top


def main(argv: list[str] | None = None) -> None:
    args = parser().parse_args(argv)
    registry = _registry()
    try:
        if args.command not in {"init", "migrate", "migrations"}:
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
