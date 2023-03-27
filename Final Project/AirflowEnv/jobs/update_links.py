import hashlib
import sys

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql import types as types

get_hash = f.udf(
    lambda a, b, c: hashlib.sha256(
        str(a).encode("utf-8") + str(b).encode("utf-8") + str(c).encode("utf-8")
    ).hexdigest()
)


def get_link_host_to_listing(links):
    link = links.withColumn("source_system_name", f.lit(source_system_name)).cache()

    link = link.join(
        ref_source_systems, on=["source_system_name"], how="left"
    ).withColumnRenamed("id", "source_id")

    link = (
        link.withColumn(
            "sk_host", get_hash(f.col("host_id"), f.lit(" host "), f.col("source_id"))
        )
        .withColumn(
            "sk_listing",
            get_hash(f.col("listing_id"), f.lit(" listing "), f.col("source_id")),
        )
        .withColumnRenamed("valid_as_of", "valid_from")
        .withColumn("valid_to_", f.lit(None))
        .withColumn(
            "link_id", get_hash(f.col("sk_host"), f.lit(" link "), f.col("sk_listing"))
        )
        .withColumn("layer", f.lit("buffer"))
    )

    link = link.withColumn("valid_to", link.valid_to_.cast(types.TimestampType()))

    link = link.select(
        "link_id", "sk_host", "sk_listing", "valid_from", "valid_to", "layer"
    )
    return link


def get_link_review_to_listing(links):
    link = (
        links.select("listing_id", f.col("id").alias("review_id"), "review_date")
        .withColumn("source_system_name", f.lit(source_system_name))
        .cache()
    )

    link = link.join(
        ref_source_systems, on=["source_system_name"], how="left"
    ).withColumnRenamed("id", "source_id")

    link = (
        link.withColumn(
            "sk_review",
            get_hash(f.col("review_id"), f.lit(" review "), f.col("review_id")),
        )
        .withColumn(
            "sk_listing",
            get_hash(f.col("listing_id"), f.lit(" listing "), f.col("source_id")),
        )
        .withColumnRenamed("review_date", "valid_from")
        .withColumn("valid_to_", f.lit(None))
        .withColumn(
            "link_id",
            get_hash(f.col("sk_review"), f.lit(" link "), f.col("sk_listing")),
        )
        .withColumn("layer", f.lit("buffer"))
    )

    link = link.withColumn("valid_to", link.valid_to_.cast(types.TimestampType()))

    link = link.select(
        "link_id", "sk_review", "sk_listing", "valid_from", "valid_to", "layer"
    )
    return link


def get_link_review_to_user(links):
    link = (
        links.select("user_id", f.col("id").alias("review_id"), "review_date")
        .withColumn("source_system_name", f.lit(source_system_name))
        .cache()
    )

    link = link.join(
        ref_source_systems, on=["source_system_name"], how="left"
    ).withColumnRenamed("id", "source_id")

    link = (
        link.withColumn(
            "sk_review",
            get_hash(f.col("review_id"), f.lit(" review "), f.col("review_id")),
        )
        .withColumn(
            "sk_user", get_hash(f.col("user_id"), f.lit(" user "), f.col("source_id"))
        )
        .withColumnRenamed("review_date", "valid_from")
        .withColumn("valid_to_", f.lit(None))
        .withColumn(
            "link_id", get_hash(f.col("sk_review"), f.lit(" link "), f.col("sk_user"))
        )
        .withColumn("layer", f.lit("buffer"))
    )

    link = link.withColumn("valid_to", link.valid_to_.cast(types.TimestampType()))

    link = link.select(
        "link_id", "sk_review", "sk_user", "valid_from", "valid_to", "layer"
    )
    return link


def fill_host_to_listing():
    df = get_link_host_to_listing(hosts)
    df.write.mode("append").saveAsTable("airbnb_dv.link_host_to_listing")


def fill_review_to_listing():
    df = get_link_review_to_listing(reviews)
    df.write.mode("append").saveAsTable("airbnb_dv.link_review_to_listing")


def fill_review_to_user():
    df = get_link_review_to_user(reviews)
    df.write.mode("append").saveAsTable("airbnb_dv.link_review_to_user")


def update_links():
    fill_host_to_listing()
    fill_review_to_listing()
    fill_review_to_user()


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

    update_links()
