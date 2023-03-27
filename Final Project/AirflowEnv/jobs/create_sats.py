import hashlib
import sys

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql import types as types


def create_tables():
    spark.sql(
        """create table airbnb_dv.sat_hosts(
                     sat_id string,
                     sk_host string,
                     host_url string,
                     host_name string,
                     response_time_id string,
                     valid_from timestamp,
                     valid_to timestamp,
                     layer string)"""
    )

    spark.sql(
        """create table airbnb_dv.sat_listings(
                     sat_id string,
                     sk_listing string,
                     listing_url string,
                     name string,
                     location_id int,
                     valid_from timestamp,
                     valid_to timestamp,
                     layer string)"""
    )

    spark.sql(
        """create table airbnb_dv.sat_reviews(
                     sat_id string,
                     sk_review string,
                     review_date string,
                     comments string,
                     valid_from timestamp,
                     valid_to timestamp,
                     layer string)"""
    )

    spark.sql(
        """create table airbnb_dv.sat_users(
                     sat_id string,
                     sk_users string,
                     user_name string,
                     comments string,
                     country_id int,
                     valid_from timestamp,
                     valid_to timestamp,
                     layer string)"""
    )


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
    # ideally you'd store all the informational cols in a sat,
    # but I'll only store some of them out convenience
    # since they don't hold any logical importance
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


def fill_hosts():
    df = get_sat_hosts(hosts)
    df.write.mode("overwrite").saveAsTable("airbnb_dv.sat_hosts")


def fill_listings():
    df = get_sat_listings(listings)
    df.write.mode("overwrite").saveAsTable("airbnb_dv.sat_hosts")


def create_sats():
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
    ref_response_times = spark.sql("select * from airbnb_dv.ref_response_times")
    ref_locations = spark.sql("select * from airbnb_dv.ref_listing_locations")

    create_sats()
