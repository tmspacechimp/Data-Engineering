import csv
import time

import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic

args = {"owner": "airflow"}

dag = DAG(
    dag_id="produce_reviews",
    default_args=args,
    schedule_interval="@once",
    start_date=airflow.utils.dates.days_ago(1),
)

reviews_path = "/airflow/data/csv/reviews_8_10_Sep_2022.csv"
topic_name = "reviews"
server = "localhost:9092"
sleep_duration = 20


def create_topic(**kwargs):
    client = KafkaAdminClient(
        bootstrap_servers=server,
    )
    topics = [NewTopic(name=topic_name, num_partitions=1, replication_factor=1)]
    client.create_topics(new_topics=topics, validate_only=False)


def write_to_kafka(**kwargs):
    producer = KafkaProducer(topic_name=topic_name)
    with open(reviews_path, "r") as file:
        reader = csv.reader(file)
        for row in reader:
            row_str = ",".join(row)
            producer.send(topic_name, row_str.encode())
            time.sleep(sleep_duration)


create_reviews_topic = PythonOperator(
    task_id="create_reviews_topic",
    python_callable=create_topic,
    dag=dag,
)


write_reviews_to_kafka = PythonOperator(
    task_id="write_reviews_to_kafka",
    python_callable=write_to_kafka,
    dag=dag,
)


create_reviews_topic >> write_reviews_to_kafka
