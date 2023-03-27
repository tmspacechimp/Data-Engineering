import jobs.hdfs_utils
from jobs import spark_utils, hdfs_utils

if __name__ == "__main__":
    spark = spark_utils.session_init()
    spark._jsc.hadoopConfiguration().set('fs.defaultFS', jobs.hdfs_utils.namenode)

    filename = hdfs_utils.find_filename("/DataLake/fakestream_1", spark)
    df = spark.read.parquet(filename)

    staging = jobs.hdfs_utils.namenode + "/user/hive/warehouse/staging"
    df.write.parquet(staging, mode="append")

    hdfs_utils.delete_from_fs(filename, spark)

