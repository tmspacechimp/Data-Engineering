import re

from airflow.models import DAG
from airflow.providers.apache.hdfs.sensors.hdfs import HdfsRegexSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="fs1_hdfs_sensor",
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
        tags=["Fakestream", "Pokemon"]
) as dag:
    sensor = HdfsRegexSensor(
        dag=dag,
        task_id="hdfs_sensor_task",
        filepath="/DataLake/fakestream_1",
        hdfs_conn_id="hdfs_conn",
        regex=re.compile("^(part-).*"))

    submit = SparkSubmitOperator(
        application="/airflow/jobs/dl_to_staging_1.py",
        task_id="dl_submit"
    )

    sensor >> submit


