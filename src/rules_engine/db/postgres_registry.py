"""PostgreSQL rule registry: the system of record for checklist rules.

Each change runs in one transaction that locks the rule's versions first, so
two people cannot activate versions of the same rule at once. The person and
reason are passed to the database (``app.actor``, ``app.reason``) and the
audit trigger in ``schema.sql`` writes them with a before and after snapshot.
The schema also refuses edits, deletes, skipped lifecycle steps, self-approval,
and overlapping versions, whoever sends the SQL.
"""

from __future__ import annotations

import threading
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from rules_engine.feature_computations import FeatureRegistry
from rules_engine.registry import check_activation, check_approval, check_retirement
from rules_engine.rule_models import AuditEvent, RuleDefinition, RuleStatus
from rules_engine.validators import validate_rule, validate_shape

SCHEMA = Path(__file__).with_name("schema.sql")
COLUMNS = (
    "rule_code, version, title, description, feature_name, operator, expected_value, mandatory, "
    "legal_reference, status, effective_from, effective_to, created_by, approved_by"
)


class RulesDatabaseError(RuntimeError):
    """The rules database is unreachable or not initialised."""


def redact(dsn: str) -> str:
    parts = urlsplit(dsn)
    if parts.password:
        host = parts.hostname or ""
        if parts.port:
            host = f"{host}:{parts.port}"
        parts = parts._replace(netloc=f"{parts.username}:***@{host}")
    return urlunsplit(parts)


def _rule(row: dict) -> RuleDefinition:
    return RuleDefinition.model_validate(row)


class PostgresRuleRegistry:
    def __init__(self, dsn: str, features: FeatureRegistry) -> None:
        self.dsn = dsn
        self.features = features
        self._conn: psycopg.Connection | None = None
        self._lock = threading.RLock()

    def _connection(self) -> psycopg.Connection:
        if self._conn is None or self._conn.closed or self._conn.broken:
            try:
                self._conn = psycopg.connect(self.dsn, autocommit=True, row_factory=dict_row, connect_timeout=5)
            except psycopg.OperationalError as exc:
                raise RulesDatabaseError(
                    f"Rules database is not reachable at {redact(self.dsn)}. Start it with: make rules-db-up"
                ) from exc
        return self._conn

    @contextmanager
    def _read(self):
        with self._lock:
            try:
                yield self._connection()
            except psycopg.OperationalError as exc:
                raise RulesDatabaseError(
                    f"Lost the rules database at {redact(self.dsn)}. Start it with: make rules-db-up"
                ) from exc

    @contextmanager
    def _change(self, actor: str, reason: str, rule_code: str | None = None):
        """One transaction, tagged with who and why, holding the rule's versions locked."""
        with self._lock:
            conn = self._connection()
            with conn.transaction():
                conn.execute("SELECT set_config('app.actor', %s, true), set_config('app.reason', %s, true)", (actor, reason))
                if rule_code is not None:
                    conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (rule_code,))
                yield conn

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    def init_schema(self) -> None:
        with self._lock:
            conn = self._connection()
            with conn.transaction():
                conn.execute(SCHEMA.read_text())

    def check(self) -> None:
        """Fail early with the command that fixes the problem."""
        with self._read() as conn:
            present = conn.execute("SELECT to_regclass('rule_versions') AS name").fetchone()["name"]
            if present is None:
                raise RulesDatabaseError("Rules database has no schema. Run: make rules-db-init")

    def is_empty(self) -> bool:
        with self._read() as conn:
            return conn.execute("SELECT NOT EXISTS (SELECT 1 FROM rule_versions) AS empty").fetchone()["empty"]

    @staticmethod
    def _fetch(conn, rule_code: str, version: int) -> RuleDefinition:
        row = conn.execute(
            f"SELECT {COLUMNS} FROM rule_versions WHERE rule_code = %s AND version = %s",
            (rule_code, version),
        ).fetchone()
        if row is None:
            raise ValueError(f"{rule_code} has no version {version}")
        return _rule(row)

    @staticmethod
    def _active(conn, rule_code: str) -> RuleDefinition | None:
        row = conn.execute(
            f"SELECT {COLUMNS} FROM rule_versions WHERE rule_code = %s AND status = 'ACTIVE'",
            (rule_code,),
        ).fetchone()
        return None if row is None else _rule(row)

    def register(self, rule: RuleDefinition, *, actor: str, reason: str = "") -> RuleDefinition:
        if rule.status != RuleStatus.DRAFT:
            raise ValueError("New rule versions must be DRAFT")
        validate_shape(rule)
        with self._change(actor, reason, rule.rule_code) as conn:
            if conn.execute(
                "SELECT 1 FROM rule_versions WHERE rule_code = %s AND status = 'RETIRED'", (rule.rule_code,)
            ).fetchone():
                raise ValueError(f"{rule.rule_code} is retired. Register the requirement under a new code")
            row = conn.execute(
                f"""
                INSERT INTO rule_versions (rule_code, version, title, description, feature_name, operator,
                                           expected_value, mandatory, legal_reference, created_by)
                SELECT %s, coalesce(max(version), 0) + 1, %s, %s, %s, %s, %s, %s, %s, %s
                FROM rule_versions WHERE rule_code = %s
                RETURNING {COLUMNS}
                """,
                (
                    rule.rule_code,
                    rule.title,
                    rule.description,
                    rule.feature_name,
                    rule.operator.value,
                    None if rule.expected_value is None else Jsonb(rule.expected_value),
                    rule.mandatory,
                    rule.legal_reference,
                    actor,
                    rule.rule_code,
                ),
            ).fetchone()
        return _rule(row)

    def approve(self, rule_code: str, version: int, *, actor: str, reason: str = "") -> RuleDefinition:
        with self._change(actor, reason, rule_code) as conn:
            target = self._fetch(conn, rule_code, version)
            check_approval(target, actor)
            validate_rule(target, self.features)
            row = conn.execute(
                f"""
                UPDATE rule_versions SET status = 'APPROVED', approved_by = %s, approved_at = now()
                WHERE rule_code = %s AND version = %s RETURNING {COLUMNS}
                """,
                (actor, rule_code, version),
            ).fetchone()
        return _rule(row)

    def activate(
        self,
        rule_code: str,
        version: int,
        effective_from: date,
        *,
        actor: str,
        reason: str = "",
    ) -> RuleDefinition:
        with self._change(actor, reason or f"in force from {effective_from}", rule_code) as conn:
            target = self._fetch(conn, rule_code, version)
            previous = self._active(conn, rule_code)
            check_activation(target, previous, effective_from)
            validate_rule(target.model_copy(update={"effective_from": effective_from}), self.features)
            if previous is not None:
                conn.execute(
                    """
                    UPDATE rule_versions SET status = 'SUPERSEDED', effective_to = %s
                    WHERE rule_code = %s AND version = %s
                    """,
                    (effective_from, rule_code, previous.version),
                )
            row = conn.execute(
                f"""
                UPDATE rule_versions SET status = 'ACTIVE', effective_from = %s
                WHERE rule_code = %s AND version = %s RETURNING {COLUMNS}
                """,
                (effective_from, rule_code, version),
            ).fetchone()
        return _rule(row)

    def retire(self, rule_code: str, effective_to: date, *, actor: str, reason: str = "") -> RuleDefinition:
        with self._change(actor, reason or f"no longer applies from {effective_to}", rule_code) as conn:
            active = check_retirement(self._active(conn, rule_code), rule_code, effective_to)
            row = conn.execute(
                f"""
                UPDATE rule_versions SET status = 'RETIRED', effective_to = %s
                WHERE rule_code = %s AND version = %s RETURNING {COLUMNS}
                """,
                (effective_to, rule_code, active.version),
            ).fetchone()
        return _rule(row)

    def get(self, rule_code: str, version: int) -> RuleDefinition:
        with self._read() as conn:
            return self._fetch(conn, rule_code, version)

    def current(self, rule_code: str) -> RuleDefinition | None:
        with self._read() as conn:
            return self._active(conn, rule_code)

    def get_active_rules(self, assessment_date: date) -> list[RuleDefinition]:
        with self._read() as conn:
            rows = conn.execute(
                f"""
                SELECT {COLUMNS} FROM rule_versions r
                WHERE status IN ('ACTIVE', 'SUPERSEDED', 'RETIRED')
                  AND effective_from <= %s AND (effective_to IS NULL OR %s < effective_to)
                ORDER BY (SELECT min(created_at) FROM rule_versions f WHERE f.rule_code = r.rule_code), rule_code
                """,
                (assessment_date, assessment_date),
            ).fetchall()
        return [_rule(row) for row in rows]

    def history(self, rule_code: str) -> list[RuleDefinition]:
        with self._read() as conn:
            rows = conn.execute(
                f"SELECT {COLUMNS} FROM rule_versions WHERE rule_code = %s ORDER BY version", (rule_code,)
            ).fetchall()
        return [_rule(row) for row in rows]

    def codes(self) -> list[str]:
        with self._read() as conn:
            rows = conn.execute(
                "SELECT rule_code FROM rule_versions GROUP BY rule_code ORDER BY min(created_at), rule_code"
            ).fetchall()
        return [row["rule_code"] for row in rows]

    def audit_log(self, rule_code: str | None = None, limit: int | None = None) -> list[AuditEvent]:
        with self._read() as conn:
            rows = conn.execute(
                """
                SELECT at, action, rule_code, version, actor, detail FROM rule_audit
                WHERE %(code)s::text IS NULL OR rule_code = %(code)s
                ORDER BY id DESC LIMIT %(limit)s
                """,
                {"code": rule_code, "limit": limit},
            ).fetchall()
        return [AuditEvent.model_validate(row) for row in reversed(rows)]

    def record_run(self, summary: dict, *, actor: str) -> int:
        """Keep a permanent record of a grant run and the rule versions that decided it."""
        with self._change(actor, "grant run") as conn:
            row = conn.execute(
                """
                INSERT INTO checklist_runs (actor, cycle, assessment_date, rules_applied, taxpayer_count,
                                            eligible, not_eligible, needs_review, summary)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
                """,
                (
                    actor,
                    summary["cycle"],
                    summary["assessment_date"],
                    list(summary["rules_applied"]),
                    summary["taxpayer_count"],
                    summary["eligible"],
                    summary["not_eligible"],
                    summary["needs_review"],
                    Jsonb(summary),
                ),
            ).fetchone()
        return int(row["id"])

    def runs(self, limit: int = 10) -> list[dict]:
        with self._read() as conn:
            return conn.execute(
                """
                SELECT id, run_at, actor, cycle, assessment_date, rules_applied, taxpayer_count,
                       eligible, not_eligible, needs_review
                FROM checklist_runs ORDER BY id DESC LIMIT %s
                """,
                (limit,),
            ).fetchall()
