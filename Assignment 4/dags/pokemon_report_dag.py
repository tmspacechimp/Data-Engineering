from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="pokemon_report",
    schedule_interval="*/15 * * * *",
    catchup=False,
    tags=["Report", "Pokemon"],
    max_active_runs=1,
) as dag:
    submit_job = SparkSubmitOperator(
        application="/airflow/jobs/pokemon_report_job.py",
        task_id="report_submit"
    )
