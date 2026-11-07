-- Checklist rules, their versions, and an append-only audit trail.
-- Safe to run more than once: make rules-db-init applies it on every call.
--
-- The database enforces what must never break, even for a hand-written UPDATE:
--   * a version's definition cannot change after it is registered
--   * status moves DRAFT -> ACTIVE -> SUPERSEDED or RETIRED
--     (DRAFT -> APPROVED -> ACTIVE is still accepted)
--   * two versions of one rule cannot be in force on the same day
--   * every insert and status change is written to rule_audit, which cannot be edited

CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE TABLE IF NOT EXISTS rule_versions (
    rule_code        text        NOT NULL CHECK (rule_code ~ '^[A-Z][A-Z0-9_]*$'),
    version          integer     NOT NULL CHECK (version >= 1),
    title            text        NOT NULL,
    description      text        NOT NULL,
    feature_name     text        NOT NULL,
    operator         text        NOT NULL CHECK (operator IN ('eq', 'ne', 'gt', 'gte', 'lt', 'lte', 'in', 'not_in', 'is_true', 'is_false')),
    expected_value   jsonb,
    mandatory        boolean     NOT NULL DEFAULT true,
    legal_reference  text,
    status           text        NOT NULL DEFAULT 'DRAFT' CHECK (status IN ('DRAFT', 'APPROVED', 'ACTIVE', 'SUPERSEDED', 'RETIRED')),
    effective_from   date,
    effective_to     date,
    created_by       text        NOT NULL,
    created_at       timestamptz NOT NULL DEFAULT clock_timestamp(),
    approved_by      text,
    approved_at      timestamptz,
    PRIMARY KEY (rule_code, version),
    CHECK (effective_to IS NULL OR effective_from IS NOT NULL AND effective_to > effective_from),
    CHECK (approved_by IS NULL OR approved_by <> created_by),
    CHECK (status IN ('DRAFT', 'APPROVED') OR effective_from IS NOT NULL),
    CHECK (status IN ('SUPERSEDED', 'RETIRED') OR effective_to IS NULL),
    EXCLUDE USING gist (rule_code WITH =, daterange(effective_from, effective_to, '[)') WITH &&)
        WHERE (effective_from IS NOT NULL)
);

CREATE UNIQUE INDEX IF NOT EXISTS one_active_version_per_rule
    ON rule_versions (rule_code) WHERE status = 'ACTIVE';

CREATE TABLE IF NOT EXISTS rule_audit (
    id         bigserial   PRIMARY KEY,
    at         timestamptz NOT NULL DEFAULT clock_timestamp(),
    action     text        NOT NULL,
    rule_code  text        NOT NULL,
    version    integer     NOT NULL,
    actor      text        NOT NULL,
    detail     text        NOT NULL DEFAULT '',
    before     jsonb,
    after      jsonb
);

CREATE INDEX IF NOT EXISTS rule_audit_rule ON rule_audit (rule_code, version, id);

-- One row per applied rules/migrations file. The checksum stops a file being edited after it ran.
CREATE TABLE IF NOT EXISTS rule_migrations (
    version      integer     PRIMARY KEY CHECK (version >= 1),
    description  text        NOT NULL,
    checksum     text        NOT NULL,
    applied_at   timestamptz NOT NULL DEFAULT clock_timestamp(),
    applied_by   text        NOT NULL
);

-- One row per grant run: which rule versions decided it, and the counts.
CREATE TABLE IF NOT EXISTS checklist_runs (
    id               bigserial   PRIMARY KEY,
    run_at           timestamptz NOT NULL DEFAULT now(),
    actor            text        NOT NULL,
    cycle            text        NOT NULL,
    assessment_date  date        NOT NULL,
    rules_applied    text[]      NOT NULL,
    taxpayer_count   bigint      NOT NULL,
    eligible         bigint      NOT NULL,
    not_eligible     bigint      NOT NULL,
    needs_review     bigint      NOT NULL,
    summary          jsonb       NOT NULL
);

CREATE OR REPLACE FUNCTION rule_versions_guard() RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.status <> 'DRAFT' OR NEW.approved_by IS NOT NULL OR NEW.effective_from IS NOT NULL THEN
            RAISE EXCEPTION 'New rule versions start as unapproved DRAFTs';
        END IF;
        IF EXISTS (SELECT 1 FROM rule_versions WHERE rule_code = NEW.rule_code AND status = 'RETIRED') THEN
            RAISE EXCEPTION '% is retired; register the requirement under a new code', NEW.rule_code;
        END IF;
        IF NEW.version <> coalesce((SELECT max(version) FROM rule_versions WHERE rule_code = NEW.rule_code), 0) + 1 THEN
            RAISE EXCEPTION '% next version must be %', NEW.rule_code,
                coalesce((SELECT max(version) FROM rule_versions WHERE rule_code = NEW.rule_code), 0) + 1;
        END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'Rule versions are kept forever; retire the rule instead';
    END IF;
    IF (NEW.rule_code, NEW.version, NEW.title, NEW.description, NEW.feature_name, NEW.operator,
        NEW.expected_value, NEW.mandatory, NEW.legal_reference, NEW.created_by, NEW.created_at)
       IS DISTINCT FROM
       (OLD.rule_code, OLD.version, OLD.title, OLD.description, OLD.feature_name, OLD.operator,
        OLD.expected_value, OLD.mandatory, OLD.legal_reference, OLD.created_by, OLD.created_at) THEN
        RAISE EXCEPTION '%:v% cannot be edited; register a new version', OLD.rule_code, OLD.version;
    END IF;
    IF NEW.status IS DISTINCT FROM OLD.status AND NOT (
        (OLD.status = 'DRAFT' AND NEW.status IN ('APPROVED', 'ACTIVE')) OR
        (OLD.status = 'APPROVED' AND NEW.status = 'ACTIVE') OR
        (OLD.status = 'ACTIVE' AND NEW.status IN ('SUPERSEDED', 'RETIRED'))
    ) THEN
        RAISE EXCEPTION '%:v% cannot move from % to %', OLD.rule_code, OLD.version, OLD.status, NEW.status;
    END IF;
    IF OLD.effective_from IS NOT NULL AND NEW.effective_from IS DISTINCT FROM OLD.effective_from THEN
        RAISE EXCEPTION '%:v% start date cannot change once in force', OLD.rule_code, OLD.version;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS rule_versions_guard ON rule_versions;
CREATE TRIGGER rule_versions_guard
    BEFORE INSERT OR UPDATE OR DELETE ON rule_versions
    FOR EACH ROW EXECUTE FUNCTION rule_versions_guard();

-- Writes the audit row for every change, including changes made outside the app.
-- The app names the person with SET LOCAL app.actor and the reason with app.reason.
CREATE OR REPLACE FUNCTION rule_versions_audit() RETURNS trigger AS $$
DECLARE
    who text := coalesce(nullif(current_setting('app.actor', true), ''), session_user);
    why text := coalesce(current_setting('app.reason', true), '');
    what text;
BEGIN
    IF TG_OP = 'INSERT' THEN
        what := 'REGISTERED';
    ELSIF NEW.status IS DISTINCT FROM OLD.status THEN
        what := CASE NEW.status WHEN 'ACTIVE' THEN 'ACTIVATED' ELSE NEW.status END;
    ELSE
        what := 'UPDATED';
    END IF;
    INSERT INTO rule_audit (action, rule_code, version, actor, detail, before, after)
    VALUES (
        what, NEW.rule_code, NEW.version, who, why,
        CASE WHEN TG_OP = 'UPDATE' THEN to_jsonb(OLD) END,
        to_jsonb(NEW)
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS rule_versions_audit ON rule_versions;
CREATE TRIGGER rule_versions_audit
    AFTER INSERT OR UPDATE ON rule_versions
    FOR EACH ROW EXECUTE FUNCTION rule_versions_audit();

CREATE OR REPLACE FUNCTION append_only() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION '% is append-only', TG_TABLE_NAME;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS rule_audit_append_only ON rule_audit;
CREATE TRIGGER rule_audit_append_only
    BEFORE UPDATE OR DELETE ON rule_audit
    FOR EACH ROW EXECUTE FUNCTION append_only();

DROP TRIGGER IF EXISTS checklist_runs_append_only ON checklist_runs;
CREATE TRIGGER checklist_runs_append_only
    BEFORE UPDATE OR DELETE ON checklist_runs
    FOR EACH ROW EXECUTE FUNCTION append_only();

DROP TRIGGER IF EXISTS rule_migrations_append_only ON rule_migrations;
CREATE TRIGGER rule_migrations_append_only
    BEFORE UPDATE OR DELETE ON rule_migrations
    FOR EACH ROW EXECUTE FUNCTION append_only();
