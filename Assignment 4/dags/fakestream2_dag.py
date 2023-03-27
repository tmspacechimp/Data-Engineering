from airflow.models import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="fakestream_2",
    schedule_interval="*/1 * * * *",
    max_active_runs=1,
    catchup=False,
        tags=["Fakestream", "Pokemon"]
) as dag:

    submit = SparkSubmitOperator(
        application="/airflow/jobs/fakestream_2_job.py",
        task_id="fs2_submit"
    )

    trigger = TriggerDagRunOperator(
        task_id="fs2_trigger_hdfs",
        trigger_dag_id="fs2_hdfs_sensor"
    )

    submit >> trigger
