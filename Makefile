.PHONY: install test serve ingest features materialize grant train score feast-ui redis-up redis-down rules-db-up rules-db-down rules-db-init rules rules-migrate mock-lake mock-lake-rm drill-check

export PYTHONPATH := src:.

# Spark 3.5 needs Java 17. A newer default java on PATH fails while the context starts.
JAVA_HOME ?= /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
export JAVA_HOME
export PATH := $(JAVA_HOME)/bin:$(PATH)

install:
	@pip install -e '.[test]'

test:
	@pytest -q

serve:
	@uvicorn api.main:app --reload --host 0.0.0.0 --port 8001

ingest:
	@python -m data_engineering.ingest

features:
	@python -m features.build

materialize:
	@python -m features.materialize

feast-ui:
	cd feature_store && PYTHONPATH=../src:.. feast ui --host 127.0.0.1 --port 8888

redis-up:
	docker compose up -d --wait redis

redis-down:
	docker compose stop redis

rules-db-up:
	docker compose up -d --wait postgres

rules-db-down:
	docker compose stop postgres

rules-db-init:
	@python -m rules_engine.cli init

rules:
	@python -m rules_engine.cli list

rules-migrate:
	@python -m rules_engine.cli migrate

grant:
	@python -m inference.grant

train:
	@python -m training.train

score:
	@python -m inference.batch_score

mock-lake:
	@python -m data_engineering.mock_lake

mock-lake-rm:
	@rm -rf mock_lake

drill-check:
	@python -m data_engineering.drill
