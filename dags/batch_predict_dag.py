"""Daily scores from the saved WHT ranker. The grant decision is unchanged."""

import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

ROOT = os.environ.get("PIPELINE_ROOT", "/opt/wht-exemption-automation")
PREFIX = f"cd '{ROOT}' && PYTHONPATH='{ROOT}/src:{ROOT}' python -m"

with DAG(
    dag_id="wht_batch_score_daily",
    start_date=datetime(2026, 1, 1),
    schedule="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["wht_exemption", "inference"],
) as dag:
    BashOperator(task_id="batch_score", bash_command=f"{PREFIX} inference.batch_score")
