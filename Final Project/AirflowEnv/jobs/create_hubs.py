import hashlib
import sys

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as f


def create_tables():
    spark.sql(
        """create table airbnb_dv.hub_hosts(
                 sk_host string,
                 host_id string,
                 source_id int)"""
    )
    spark.sql(
        """create table airbnb_dv.hub_listings(
                 sk_listing string,
                 listing_id string,
                 source_id int)"""
    )
    spark.sql(
        """create table airbnb_dv.hub_reviews(
                 sk_review string,
                 review_id string,
                 source_id int)"""
    )
    spark.sql(
        """create table airbnb_dv.hub_users(
                 sk_user string,
                 user_id string,
                 source_id int)"""
    )


get_hash = f.udf(
    lambda a, b, c: hashlib.sha256(
        str(a).encode("utf-8") + str(b).encode("utf-8") + str(c).encode("utf-8")
    ).hexdigest()
)


def get_hub_hosts(hosts):
    hub = (
        hosts.select(f.col("host_id"))
        .withColumn("source_system_name", f.lit(source_system_name))
        .cache()
    )

    hub = hub.join(
        ref_source_systems, on=["source_system_name"], how="left"
    ).withColumnRenamed("id", "source_id")

    hub = hub.withColumn(
        "sk_host", get_hash(hub["host_id"], f.lit(" host "), hub["source_id"])
    )

    hub = hub.select("sk_host", "host_id", "source_id")

    return hub


def get_hub_listings(listings):
    hub = (
        listings.select("id")
        .withColumn("source_system_name", f.lit(source_system_name))
        .withColumnRenamed("id", "listing_id")
        .cache()
    )

    hub = hub.join(
        ref_source_systems, on=["source_system_name"], how="left"
    ).withColumnRenamed("id", "source_id")
    hub = hub.withColumn(
        "sk_listing",
        get_hash(f.col("listing_id"), f.lit(" listing "), f.col("source_id")),
    )

    hub = hub.select("sk_listing", "listing_id", "source_id")

    return hub


def fill_hosts():
    df = get_hub_hosts(hosts)
    df.write.mode("overwrite").saveAsTable("airbnb_dv.hub_hosts")


def fill_listings():
    df = get_hub_listings(listings)
    df.write.mode("overwrite").saveAsTable("airbnb_dv.hub_listings")


def create_hubs():
    create_tables()
    fill_hosts()
    fill_listings()


if __name__ == "__main__":
    app_name = "hive_init"
    conf = SparkConf()

    hdfs_host = "hdfs://namenode:8020"

    conf.set("hive.metastore.uris", "http://hive-metastore:9083")
    conf.set("spark.kerberos.access.hadoopFileSystem", hdfs_host)
    conf.set("spark.sql.warehouse.dir", f"{hdfs_host}/user/hive/warehouse")
    conf.set("hive.metastore.warehouse.dir", f"{hdfs_host}/user/hive/warehouse")

    conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")

    conf.setMaster("local[*]")

    spark = (
        SparkSession.builder.appName(app_name)
        .config(conf=conf)
        .enableHiveSupport()
        .getOrCreate()
    )

    existing_hosts_path = "/airflow/data/csv/hosts_existing_7_Sep_2022.csv"
    existing_listings_path = "/airflow/data/csv/listings_existing_7_Sep_2022.csv"

    hosts = spark.read.option("header", True).csv(existing_hosts_path)
    listings = spark.read.option("header", True).csv(existing_listings_path)

    source_system_name = sys.argv[1]
    ref_source_systems = spark.sql("select * from airbnb_dv.ref_source_systems")

    create_hubs()
