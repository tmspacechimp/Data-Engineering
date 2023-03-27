import jobs.hdfs_utils
import spark_utils

if __name__ == '__main__':
    spark = spark_utils.session_init()
    path = "airflow/data/pokemon1.csv"
    df = spark.read.option("header", True).csv(path)

    row = spark_utils.fs_row_picker(df, "1")
    row.withColumn("type", 1)

    row.write.paquet(jobs.hdfs_utils.namenode + "/DataLake/fakestream_1",
                     mode="append")
