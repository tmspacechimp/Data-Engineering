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


def get_sat_hosts(hosts):
    # ideally you'd store all the informational cols in a sat,
    # but I'll only store some of them out convenience
    # since they don't hold any logical importance
    sat = (
        hosts.select(
            "host_id",
            "host_url",
            "host_name",
            f.col("host_response_time"),
            "valid_as_of",
        )
        .withColumn("source_system_name", f.lit(source_system_name))
        .cache()
    )

    sat = sat.join(
        ref_source_systems, on=["source_system_name"], how="left"
    ).withColumnRenamed("id", "source_id")

    sat = (
        sat.join(ref_response_times, on=["host_response_time"], how="left")
        .withColumnRenamed("id", "response_time_id")
        .cache()
    )

    sat = sat.fillna(0, subset=["response_time_id"])

    sat = (
        sat.withColumn(
            "sk_host", get_hash(f.col("host_id"), f.lit(" host "), f.col("source_id"))
        )
        .withColumnRenamed("valid_as_of", "valid_from_")
        .withColumn("valid_to_", f.lit(None))
        .withColumn("layer", f.lit("processed"))
        .withColumn(
            "sat_id", get_hash(f.col("sk_host"), f.lit(" sat "), f.lit(" host "))
        )
    )

    sat = sat.withColumn("valid_to", sat.valid_to_.cast(types.TimestampType()))
    # valid_from gets mostly filled with strings so I have to do it manually
    sat = sat.withColumn("valid_from", f.lit("7-9-2022 12:00:00.0"))

    sat = sat.select(
        "sat_id",
        "sk_host",
        "host_url",
        "host_name",
        "response_time_id",
        "valid_from",
        "valid_to",
        "layer",
    )
    return sat


def get_sat_listings(listings):
    sat = (
        listings.withColumnRenamed("id", "listing_id")
        .select(
            "listing_id",
            "listing_url",
            "name",
            f.col("neighbourhood").alias("location_name"),
            "valid_as_of",
        )
        .withColumn("source_system_name", f.lit(source_system_name))
        .cache()
    )

    sat = sat.join(
        ref_source_systems, on=["source_system_name"], how="left"
    ).withColumnRenamed("id", "source_id")

    sat = sat.join(ref_locations, on=["location_name"], how="left").withColumnRenamed(
        "id", "location_id"
    )

    sat = sat.fillna(0, subset=["location_id"])

    sat = (
        sat.withColumn(
            "sk_listing",
            get_hash(f.col("listing_id"), f.lit(" listing "), f.col("source_id")),
        )
        .withColumnRenamed("valid_as_of", "valid_from_")
        .withColumn("valid_to_", f.lit(None))
        .withColumn("layer", f.lit("processed"))
        .withColumn(
            "sat_id", get_hash(f.col("sk_listing"), f.lit(" sat "), f.lit(" listing "))
        )
    )

    sat = sat.withColumn("valid_to", sat.valid_to_.cast(types.TimestampType()))
    # valid_from gets mostly filled with strings so I have to do it manually
    sat = sat.withColumn("valid_from", f.lit("7-9-2022 12:00:00.0"))

    sat = sat.select(
        "sat_id",
        "sk_listing",
        "listing_url",
        "name",
        "location_id",
        "valid_from",
        "valid_to",
        "layer",
    )

    return sat


def get_sat_users(reviews):
    sat = (
        reviews.select("user_id", "user_name", f.col("user_country").alias("country"))
        .withColumn("source_system_name", f.lit(source_system_name))
        .cache()
    )

    sat = sat.join(
        ref_source_systems, on=["source_system_name"], how="left"
    ).withColumnRenamed("id", "source_id")

    ref_country = None  # so it's not an error
    sat = sat.join(ref_country, on=["country"], how="left").withColumnRenamed(
        "id", "country_id"
    )

    sat = sat.fillna(0, subset=["country_id"])

    sat = (
        sat.withColumn(
            "sk_user", get_hash(f.col("user_id"), f.lit(" user "), f.col("source_id"))
        )
        .withColumn("valid_to_", f.lit(None))
        .withColumn("layer", f.lit("buffer"))
        .withColumn(
            "sat_id", get_hash(f.col("sk_user"), f.lit(" sat "), f.lit(" user "))
        )
    )

    sat = sat.withColumn("valid_to", sat.valid_to_.cast(types.TimestampType()))
    # valid_from gets mostly filled with strings so I have to do it manually
    sat = sat.withColumn("valid_from", f.lit("8-9-2022 12:00:00.0"))

    sat = sat.select(
        "sat_id",
        "sk_user",
        "user_name",
        "country_id",
        "valid_from",
        "valid_to",
        "layer",
    )

    return sat


def get_sat_reviews(reviews):
    sat = (
        reviews.withColumnRenamed("id", "review_id")
        .select("review_id", "review_date", "comments")
        .withColumn("source_system_name", f.lit(source_system_name))
        .cache()
    )

    sat = sat.join(
        ref_source_systems, on=["source_system_name"], how="left"
    ).withColumnRenamed("id", "source_id")

    sat = (
        sat.withColumn(
            "sk_review",
            get_hash(f.col("review_id"), f.lit(" review "), f.col("source_id")),
        )
        .withColumn("valid_to_", f.lit(None))
        .withColumn("layer", f.lit("buffer"))
        .withColumn(
            "sat_id", get_hash(f.col("sk_review"), f.lit(" sat "), f.lit(" review "))
        )
    )

    sat = sat.withColumn("valid_to", sat.valid_to_.cast(types.TimestampType()))
    # valid_from gets mostly filled with strings so I have to do it manually
    sat = sat.withColumn("valid_from", f.lit("8-9-2022 12:00:00.0"))

    sat = sat.select(
        "sat_id",
        "sk_review",
        "review_date",
        "comments",
        "valid_from",
        "valid_to",
        "layer",
    )

    return sat


def fill_hosts():
    df = get_sat_hosts(hosts)
    df.write.mode("append").saveAsTable("airbnb_dv.sat_hosts")


def fill_listings():
    df = get_sat_listings(listings)
    df.write.mode("append").saveAsTable("airbnb_dv.sat_hosts")


def fill_reviews():
    df = get_sat_reviews(reviews)
    df.write.mode("append").saveAsTable("airbnb_dv.sat_reviews")


def fill_users():
    df = get_sat_users(reviews)
    df.write.mode("append").saveAsTable("airbnb_dv.sat_users")


def create_sats():
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
    ref_response_times = spark.sql("select * from airbnb_dv.ref_response_times")
    ref_locations = spark.sql("select * from airbnb_dv.ref_listing_locations")
