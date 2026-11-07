"""Daily taxpayer load, ranker features, and grant table."""

import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

ROOT = os.environ.get("PIPELINE_ROOT", "/opt/wht-exemption-automation")
PREFIX = f"cd '{ROOT}' && PYTHONPATH='{ROOT}/src:{ROOT}' python -m"

with DAG(
    dag_id="wht_features_daily",
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["wht_exemption", "features"],
) as dag:
    ingest = BashOperator(task_id="ingest_taxpayers", bash_command=f"{PREFIX} data_engineering.ingest")
    features = BashOperator(task_id="build_features", bash_command=f"{PREFIX} features.build")
    materialize = BashOperator(
        task_id="materialize_features",
        bash_command=f"{PREFIX} features.materialize",
    )
    grant = BashOperator(task_id="build_grant", bash_command=f"{PREFIX} inference.grant")
    ingest >> features >> materialize
    features >> grant
