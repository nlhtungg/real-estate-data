from airflow import DAG
from airflow.operators.bash import BashOperator # type: ignore
from airflow.operators.dummy import DummyOperator # type: ignore
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
    catchup=False
) as dag:

    crawl_job = BashOperator(
        task_id="run_crawler",
        bash_command="python3 /opt/airflow/ingest/crawler.py"
    )

    transform_job = BashOperator(
        task_id='run_my_spark_application',
        bash_command='''
        docker exec spark-master spark-submit \
            --master spark://spark-master:7077 \
            --conf spark.executor.memory=2g \
            --conf spark.driver.memory=1g \
            --name airflow-spark-job \
            --deploy-mode client \
            /opt/transform/transform.py
        '''
    )

    success = DummyOperator(task_id="success")
    fail = DummyOperator(task_id="fail", trigger_rule="one_failed")

    crawl_job >> transform_job >> success
    [crawl_job, transform_job] >> fail