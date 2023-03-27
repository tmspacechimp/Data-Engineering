import jobs.hdfs_utils
import spark_utils

if __name__ == '__main__':
    spark = spark_utils.session_init()
    path = "airflow/data/pokemon2.csv"
    df = spark.read.option("header", True).csv(path)

    row = spark_utils.fs_row_picker(df, "2")
    row.withColumn("type", 1)

    row.write.paquet(jobs.hdfs_utils.namenode + "/DataLake/fakestream_2",
                     mode="append")
