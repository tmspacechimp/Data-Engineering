from pyspark import SparkConf
from pyspark.sql import SparkSession


def create_db(spark):
    spark.sql("create database airbnb_dv")


# add for property_type, room_type, bathroom_type, amenity_type,


def main(*args, **kwargs):
    app_name = "hive_init"
    conf = SparkConf()

    hdfs_host = "hdfs://namenode:8020"

    conf.set("hive.metastore.uris", "http://hive-metastore:9083")
    conf.set("spark.kerberos.access.hadoopFileSystem", hdfs_host)
    conf.set("spark.sql.warehouse.dir", f"{hdfs_host}/user/hive/warehouse")
    conf.set("hive.metastore.warehouse.dir", f"{hdfs_host}/user/hive/warehouse")

    conf.setMaster("local[*]")

    spark = (
        SparkSession.builder.appName(app_name)
        .config(conf=conf)
        .enableHiveSupport()
        .getOrCreate()
    )

    create_db(spark)


if __name__ == "__main__":
    main()
