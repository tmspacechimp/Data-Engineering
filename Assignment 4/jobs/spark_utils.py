from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, rand, lit, concat, col
from pyspark.sql.types import StringType

from jobs.hdfs_utils import namenode


def session_init():
    conf = SparkConf()
    conf.set("spark.kerberos.access.hadoopFileSystem",
             namenode)
    conf.set("spark.sql.warehouse.dir",
             namenode + "/user/hive/warehouse")
    conf.set("hive.metastore.uris",
             "http://hive-metastore:9083")
    conf.set("hive.metastore.warehouse.dir",
             namenode + "/user/hive/warehouse")
    conf.setMaster("local[*]")

    return SparkSession\
        .builder\
        .appName("Pokemon")\
        .config(conf=conf)\
        .enableHiveSupport()\
        .getOrCreate()


def fs_row_picker(df, fsn):
    df = df.withColumn("row_num", monotonically_increasing_id())
    df = df.withColumn("row_num", df.row_num.cast(StringType()))
    df = df.orderBy(rand()).limit(1)
    df = df.withColumnRenamed("row_num", "uid")
    return df.withColumn("uid", concat(col("uid"), lit("_" + fsn)))

