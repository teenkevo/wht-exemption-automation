"""Weekly WHT similarity ranker. The grant rules stay on the daily run."""

import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

ROOT = os.environ.get("PIPELINE_ROOT", "/opt/wht-exemption-automation")
PREFIX = f"cd '{ROOT}' && PYTHONPATH='{ROOT}/src:{ROOT}' python -m"

with DAG(
    dag_id="wht_train_weekly",
    start_date=datetime(2026, 1, 1),
    schedule="0 4 * * 0",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=10)},
    tags=["wht_exemption", "training"],
) as dag:
    BashOperator(task_id="train_ranker", bash_command=f"{PREFIX} training.train")
