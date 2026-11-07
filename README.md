# WHT exemption automation

Load, grant, rank, and lookup for withholding-tax exemption. This directory is the production layout for that project. `src/wht_exemption` in the parent repository stays as the project under development and is not imported from here.

`make features` registers the snapshot in Feast. `make materialize` copies each TIN into the Redis online store. `POST /predict` reads one TIN's features from Redis and scores them with the model named in `serving.model_name`. The served decision is `POST /grant`, from the batch grant table, with the evidence for every rule. A ranker score can sit beside that decision and does not change it.

## Layout

| Path | Role |
|---|---|
| `config/` | Exemption cycles, lake source paths, and Spark settings |
| `src/data_engineering/` | Build `wht_base_v1` from seven lake tables, then keep one row per TIN |
| `src/rules_engine/` | Checklist rules, the features they read, the PostgreSQL store, the engine, and the CLI |
| `src/features/` | Leakage-safe ranker features and the Feast registry. There is no second entity |
| `src/training/` | Random forest, logistic regression, and gradient-boosted trees |
| `src/inference/` | Grant table and batch ranker scores |
| `api/` | Health, grant lookup, checklist rules, and online ranker scoring |
| `feature_store/` | Feast view registered from the ranker snapshot |
| `dags/` | Daily load, features, and grant; weekly training; daily scores |

## What runs today

1. `make ingest` reads registration, ledger, payments, EFRIS, sector, customs headers, and filed returns. Registration is the spine. Each other source becomes one row per TIN. Sector duplicates keep one activity code. The checklist label (`wht_eligible`) from the rules engine is stored on the table.
2. `make features` writes the ranker snapshot under `data/features` and registers that view in Feast. Redis must be running: `make redis-up`.
3. `make materialize` copies the snapshot into Redis at `localhost:6379`. It writes 100,000 TINs per batch and prints progress. About 6.6 million TINs take roughly six minutes and 5 GB of Redis memory. `REDIS_URL` points a deployment at another Redis. `online_store.type: sqlite` in `config.yaml` keeps a local file instead.
4. `make grant` runs the rules engine over every TIN and writes one row each: the feature behind every rule, each rule's outcome (PASS, FAIL, or REVIEW), the checklist status, the decision, and the rule versions used. Rules come from the PostgreSQL rules database, and each run is recorded there. A mandatory FAIL makes the taxpayer NOT ELIGIBLE. A missing value sends the rule to REVIEW and the taxpayer to NEEDS REVIEW. A TIN with no rows in a fact table counts as zero for that table's counts and amounts. Cycles in `config/config.yaml` follow the financial year, which starts on 1 July: `wht_2026_27_h1` runs from 1 July to 31 December 2026 (Q1 and Q2), and `wht_2026_27_h2` runs from 1 January to 30 June 2027 (Q3 and Q4). The run date picks the cycle and is the assessment date. `WHT_AS_OF` reruns an earlier date and `WHT_CYCLE` names a cycle directly.
5. `make train` fits the three models. Reports contain counts, not TINs.
6. `make score` writes a similarity score and a High, Medium, or Low tier.
7. `make serve` looks up the grant table on port 8001 and serves the checklist rules API.

`WHT_MODE` is `local` or `remote`. Remote reads the seven lake tables in `config.yaml`, joins them, and saves `data/delta/wht_base_v1`. Local skips the lake. Features, the grant, training, and scores read that saved table in either mode. The cluster sets `WHT_MODE=remote` in `config/airflow.env`.

The daily DAG loads the taxpayers, registers the features, and materializes them at 02:00. The grant is built from that same run. Training is weekly. Batch scores run at 06:00.

## Local run

Python 3.11, Java 17, and Docker are required. Redis and PostgreSQL run in Docker from `docker-compose.yaml`. Redis and needs about 5 GB free in Docker Desktop's memory limit (Settings → Resources) for the full load.

```bash
python3.11 -m venv .venv
source .venv/bin/activate
make install
make redis-up
make rules-db-up
make rules-db-init
make test
make serve
```

```bash
curl -s http://127.0.0.1:8001/health
curl -s http://127.0.0.1:8001/ready
curl -s -X POST http://127.0.0.1:8001/grant -H 'Content-Type: application/json' -d '{"tin":"<TIN>"}'
curl -s -X POST http://127.0.0.1:8001/predict -H 'Content-Type: application/json' -d '{"entity_id":"<TIN>"}'
```

`/ready` stays unavailable until `make grant` has written the table. `/grant` returns 503 while the rules database is down. `/predict` needs `make materialize`, `make train`, Redis, and Java 17 in `JAVA_HOME`. The server loads the model into a single-core local Spark session at startup, which takes about six seconds; each request then takes about a quarter of a second. It returns 404 for a TIN missing from Redis and 503 when Redis or the model is not available.

## Checklist rules

A rule names a feature, an operator, and an expected value. It has no code of its own.

| File | Role |
|---|---|
| `src/rules_engine/rule_models.py` | Rule definition, rule result, statuses, operators, audit events |
| `src/rules_engine/validators.py` | Checks a rule against its feature before activation; PASS, FAIL, or REVIEW for one taxpayer and the same logic as a Spark column |
| `src/rules_engine/registry.py` | The lifecycle checks, plus an in-memory registry for tests and offline runs |
| `src/rules_engine/db/` | The PostgreSQL registry (`postgres_registry.py`) and its schema (`schema.sql`) |
| `src/rules_engine/feature_computations/` | Where each feature's data comes from and how it is computed |
| `src/rules_engine/cli.py` | Register, approve, activate, retire, and inspect rules (`make rules`) |
| `src/rules_engine/engine.py` | Applies the versions in force on a date to one taxpayer or to the whole table |
| `src/rules_engine/catalog.py` | Opens the rules database. It ships with no rules |
| `src/rules_engine/cycles.py` | Which rules each exemption cycle switches on |

### Rules database

PostgreSQL 16 at `postgresql://wht:wht@localhost:5432/wht_rules` (`rules_store` in `config.yaml`; `RULES_DB_URL` in a deployment) is the system of record. `make rules-db-init` creates the schema and leaves it empty. Rules are registered with the CLI or the checklist API. A cycle in `config.yaml` names the codes to switch on. `make grant` refuses to run if any switched-on code has no version in force on the assessment date, including a misspelled code. It does not write a grant table in that case.

A version moves DRAFT → APPROVED → ACTIVE, then SUPERSEDED when a newer version takes over or RETIRED when the rule ends. The database itself enforces this, so a direct SQL session gets the same answer as the app:

- A version is never edited or deleted. A change is a new version.
- The person who registered a version cannot approve it.
- Versions of one rule never overlap in time, and only one is ACTIVE.
- A start date is never moved or backdated before the current version.
- A retired code cannot be registered again.
- Every change writes a row to `rule_audit`: action, who (`--actor`, or the database user for direct SQL), why (`--reason`), and the row before and after. `rule_audit` and `checklist_runs` are append-only.
- Each `make grant` adds a row to `checklist_runs` with the cycle, assessment date, rule versions, and counts.

Changing a threshold, for example raising the tax collected minimum from 1 January 2027:

```bash
cat > min_tax_v2.yaml <<'YAML'
rule_code: MIN_TAX_COLLECTED
title: Tax collected at least 150M
description: Total tax paid is at least UGX 150,000,000
feature_name: tax_collected
operator: gte
expected_value: 150000000.0
YAML
python -m rules_engine.cli register min_tax_v2.yaml --actor alice --reason "Policy memo 12/2026" --rm
python -m rules_engine.cli approve MIN_TAX_COLLECTED 2 --actor bob --reason "Checked against memo"
python -m rules_engine.cli activate MIN_TAX_COLLECTED 2 --from 2027-01-01 --actor bob
```

Version 1 still decides assessments before 1 January 2027, so reruns of earlier dates give the same answer. `python -m rules_engine.cli retire CODE --from DATE` ends a rule. `list --on DATE`, `history CODE`, `audit [CODE]`, and `runs` show the state and the trail. A new rule code also needs switching on in the cycle in `config.yaml`. A requirement that needs new data starts with a feature in `src/rules_engine/feature_computations/`, which must read a column on the joined table. `register` refuses the rule until that feature exists.

### Checklist API

`make serve` exposes the same operations as the rules CLI at `/api/v1/checklist`. A write sends the person in `X-Actor` and the reason in `X-Reason`. The author of a version cannot approve it.

| Method and path | What it does |
|---|---|
| `GET /rules/active?on=YYYY-MM-DD` | Rules in force on a date. Today when `on` is omitted |
| `GET /rules/{code}` | Every version of one rule |
| `GET /rules/{code}/versions/{version}` | One version |
| `GET /audit?code=&limit=` | Who changed what, and why |
| `GET /runs?limit=` | Grant runs and the versions they used |
| `POST /rules` | Register the next DRAFT version |
| `POST /rules/{code}/versions/{version}/approve` | A different person signs it |
| `POST /rules/{code}/versions/{version}/activate` | In force from `effective_from` |
| `POST /rules/{code}/retire` | Stops applying on `effective_from` |

The rules database answers 503 while it is down. A refused change answers 422, and a change the database constraints reject answers 409.

Tests use the in-memory registry (`RULES_STORE=memory`). `tests/integration/test_rules_postgres.py` runs against a scratch `wht_rules_test` database and is skipped when PostgreSQL is not running.
