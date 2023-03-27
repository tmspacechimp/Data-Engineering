import hashlib
import sys

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

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


def get_hub_reviews(reviews):
    hub = (
        reviews.select("id")
        .withColumn("source_system_name", f.lit(source_system_name))
        .withColumnRenamed("id", "review_id")
    )

    hub = hub.join(
        ref_source_systems, on=["source_system_name"], how="left"
    ).withColumnRenamed("id", "source_id")
    hub = hub.withColumn(
        "sk_review", get_hash(f.col("review_id"), f.lit(" review "), f.col("source_id"))
    )

    hub = hub.select("sk_review", "review_id", "source_id")

    return hub


def get_hub_users(reviews):
    hub = reviews.select("user_id").withColumn(
        "source_system_name", f.lit(source_system_name)
    )
    hub = hub.join(
        ref_source_systems, on=["source_system_name"], how="left"
    ).withColumnRenamed("id", "source_id")
    hub = hub.withColumn(
        "sk_user", get_hash(f.col("user_id"), f.lit(" user "), f.col("source_id"))
    )

    hub = hub.select("sk_user", "user_id", "source_id")

    return hub


def fill_hosts():
    df = get_hub_hosts(hosts)
    df.write.mode("append").saveAsTable("airbnb_dv.hub_hosts")


def fill_listings():
    df = get_hub_listings(listings)
    df.write.mode("append").saveAsTable("airbnb_dv.hub_listings")


def fill_reviews():
    df = get_hub_reviews(reviews)
    df.write.mode("append").saveAsTable("airbnb_dv.hub_reviews")


def fill_users():
    df = get_hub_users(reviews)
    df.write.mode("append").saveAsTable("airbnb_dv.hub_users")


def update_hubs():
    fill_hosts()
    fill_listings()
    fill_reviews()
    fill_users()


if __name__ == "__main__":
    app_name = "hive_update"
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
    og_reviews_path = "/airflow/data/csv/reviews_8_10_Sep_2022.csv"

    hosts_schema = spark.read.schema().csv(existing_hosts_path)
    listings_schema = spark.read.schema().csv(existing_listings_path)
    reviews_schema = spark.read.schema().csv(og_reviews_path)

    source_system_name = sys.argv[1]
    hosts_path = sys.argv[2]
    listings_path = sys.argv[3]
    reviews_path = sys.argv[4]

    hosts = spark.read.option("header", False).schema(hosts_schema).csv(hosts_path)

    listings = (
        spark.read.option("header", False).schema(listings_schema).csv(listings_path)
    )
    reviews = (
        spark.read.option("header", False).schema(reviews_schema).csv(reviews_path)
    )

    ref_source_systems = spark.sql("select * from airbnb_dv.ref_source_systems")

    update_hubs()
