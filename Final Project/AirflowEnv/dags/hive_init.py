from datetime import timedelta

import airflow
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

args = {"owner": "airflow"}


dag = DAG(
    dag_id="hive_init",
    default_args=args,
    schedule_interval="@once",
    start_date=airflow.utils.dates.days_ago(1),
)


create_db = SparkSubmitOperator(
    application="/airflow/jobs/create_db.py",
    task_id="create_db",
    application_args=[],
    dag=dag,
)

create_refs = SparkSubmitOperator(
    application="/airflow/jobs/create_refs.py",
    task_id="create_refs",
    application_args=[],
    dag=dag,
)

create_hubs = SparkSubmitOperator(
    application="/airflow/jobs/create_hubs.py",
    task_id="create_hubs",
    application_args=["airbnb_netherlands"],
    dag=dag,
)

create_links = SparkSubmitOperator(
    application="/airflow/jobs/create_links.py",
    task_id="create_links",
    application_args=["airbnb_netherlands"],
    dag=dag,
)

create_sats = SparkSubmitOperator(
    application="/airflow/jobs/create_sats.py",
    task_id="create_sats",
    application_args=["airbnb_netherlands"],
    dag=dag,
)


create_db >> create_refs
create_refs >> create_hubs
create_refs >> create_links
create_refs >> create_sats




