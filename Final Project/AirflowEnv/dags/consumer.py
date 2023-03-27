import csv
import os
import time

import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic

args = {"owner": "airflow"}

topics = ["hosts_transactions", "listings_transactions", "reviews"]
file_paths = {
    "hosts_transactions": "/airflow/data/csv/tmp_hosts.csv",
    "listings_transactions": "/airflow/data/csv/tmp_listings.csv",
    "reviews": "/airflow/data/csv/tmp_reviews.csv",
}
server = "localhost:9092"


dag = DAG(
    dag_id="hive_init",
    default_args=args,
    schedule_interval="*/5 * * * *",
    start_date=airflow.utils.dates.days_ago(1),
    catchup=False,
)


def create_files(**kwargs):
    for _, path in file_paths:
        open(path, "w")


def consume_and_write(**kwargs):
    consumer = KafkaConsumer(
        topics=topics, bootstrap_servers=server, auto_offset_reset="earliest"
    )
    while True:
        record = consumer.poll(200)
        if record:
            with open(file_paths[record["topic"]], "a") as f:
                f.write(record["value"].decode("utf-8") + "\n")
        else:
            break


def delete_files(**kwargs):
    for _, path in file_paths:
        if os.path.exists(path):
            os.remove(path)


create_temp_files = PythonOperator(
    task_id="create_temp_files",
    python_callable=create_files,
    dag=dag,
)


consume_and_write_to_hdfs = PythonOperator(
    task_id="create_temp_files",
    python_callable=consume_and_write,
    dag=dag,
)


update_hubs = SparkSubmitOperator(
    application="/airflow/jobs/update_hubs.py",
    task_id="update_tables",
    application_args=[
        "airbnb_netherlands",
        file_paths["hosts_transactions"],
        file_paths["listings_transactions"],
        file_paths["reviews"],
    ],
    dag=dag,
)

update_links = SparkSubmitOperator(
    application="/airflow/jobs/update_links.py",
    task_id="update_tables",
    application_args=[
        "airbnb_netherlands",
        file_paths["hosts_transactions"],
        file_paths["listings_transactions"],
        file_paths["reviews"],
    ],
    dag=dag,
)

update_sats = SparkSubmitOperator(
    application="/airflow/jobs/update_sats.py",
    task_id="update_tables",
    application_args=[
        "airbnb_netherlands",
        file_paths["hosts_transactions"],
        file_paths["listings_transactions"],
        file_paths["reviews"],
    ],
    dag=dag,
)


delete_temp_files = SparkSubmitOperator(
    task_id="delete_temp_files",
    python_callable=delete_files,
    dag=dag,
)


create_temp_files >> consume_and_write_to_hdfs

consume_and_write_to_hdfs >> update_hubs
consume_and_write_to_hdfs >> update_sats
consume_and_write_to_hdfs >> update_links

update_hubs >> delete_temp_files
update_sats >> delete_temp_files
update_links >> delete_temp_files

