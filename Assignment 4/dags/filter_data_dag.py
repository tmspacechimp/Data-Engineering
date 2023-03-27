from datetime import timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from dags.external_task_sensor import ExternalTaskSensor

with DAG(
    dag_id="filter_data",
    schedule_interval="*/5 * * * *",
    dagrun_timeout=timedelta(seconds=120),
    catchup=False,
    max_active_runs=2,
    tags=["Filter", "Pokemon"]
) as dag:
    sensor = ExternalTaskSensor(
        dag=dag,
        task_id="external_task_sensor",
        external_task_id="submit_job",
        external_tasks=[
            ("fs1_hdfs_sensor", "dl_submit"),
            ("fs2_hdfs_sensor", "dl_submit")],
        poke_interval=6
    )

    submit = SparkSubmitOperator(
        application="/airflow/jobs/filter_data_job.py",
        task_id="filter_submit",
    )

    sensor >> submit

