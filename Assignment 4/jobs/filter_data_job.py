from jobs import spark_utils, hdfs_utils

if __name__ == '__main__':
    spark = spark_utils.session_init()
    spark._jsc.hadoopConfiguration().set('fs.defaultFS', hdfs_utils.namenode)

    staging = "/user/hive/warehouse/staging"

    filenames = [name for name in hdfs_utils.list_files(staging, spark)
                 if name.endswith(".parquet")]
    df = spark.read.parquet(filenames[0])
    for name in filenames[0:]:
        cur = spark.read.parquet(name)
        df = df.union(cur)

    df = df.dropDuplicates(["uid"])
    df = df.drop("uid")
    df.write.option("path", staging + "/filteredData")\
        .saveAsTable("pokemon", format="parquet", mode="overwrite")
