from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import pendulum

local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

default_args = {
    "owner": "data_engineer",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="crawl_daily",
    default_args=default_args,
    start_date=datetime(2025, 8, 4, tzinfo=local_tz),
    schedule_interval="0 9 * * *",  # mỗi ngày lúc 9h sáng
    catchup=False,
    tags=["alonhadat", "crawler"]
) as dag:

    crawl_job = BashOperator(
        task_id="run_crawler",
        bash_command="python3 /opt/airflow/ingest/crawler.py"
    )
    filter_job = BashOperator(
        task_id="run_filter",
        bash_command="python3 /opt/airflow/ingest/filter.py"
    )
    success = DummyOperator(task_id="success")
    fail = DummyOperator(task_id="fail", trigger_rule="one_failed")

    crawl_job >> filter_job
    filter_job >> success
    filter_job >> fail