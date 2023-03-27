from airflow.models import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="fakestream_1",
    schedule_interval='*/1 * * * *',
    max_active_runs=1,
    catchup=False,
        tags=["Fakestream", "Pokemon"]
) as dag:

    submit = SparkSubmitOperator(
        application="/airflow/jobs/fakestream_1_job.py",
        task_id="fs1_submit"
    )

    trigger = TriggerDagRunOperator(
        task_id="fs1_trigger_hdfs",
        trigger_dag_id="fs1_hdfs_sensor"
    )

    submit >> trigger
