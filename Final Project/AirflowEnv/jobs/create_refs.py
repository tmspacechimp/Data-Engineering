from pyspark import SparkConf
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as f
from pyspark.sql.types import *


def create_tables():
    spark.sql(
        """create table airbnb_dv.ref_source_systems(id int,
                 source_system_name string)"""
    )
    spark.sql(
        """insert into airbnb_dv.ref_source_systems values
                 (1, 'airbnb_netherlands')"""
    )

    spark.sql(
        """create table airbnb_dv.ref_listing_locations(id int,
                 location_name string)"""
    )

    spark.sql(
        """create table airbnb_dv.ref_countries(id int,
                 country string)"""
    )

    spark.sql(
        """create table airbnb_dv.ref_property_types(id int,
                 property_type string)"""
    )

    spark.sql(
        """create table airbnb_dv.ref_response_times(id int,
                 host_response_time string)"""
    )


def get_listing_locations(listings):
    schema = StructType([StructField("location_name", StringType(), True)])
    na = spark.createDataFrame([("N/A",)], schema)

    locations = listings.selectExpr("neighbourhood as location_name")
    locations = locations.fillna("N/A").union(na).distinct()
    # for some reason data from other columns which have long texts in them
    # overlap to this column or it's just a bad csv, so I've filtered some of them out
    locations = (
        locations.filter(~locations.location_name.rlike("^[^a-zA-Z].*"))
        .filter(~locations.location_name.contains("https"))
        .filter(f.expr("length(location_name) <= 100"))
    )
    locations = locations.select(
        "*",
        f.row_number()
        .over(Window.orderBy(f.monotonically_increasing_id()))
        .alias("id"),
    )
    locations = locations.selectExpr("id", "location_name")

    return locations


def get_ref_response_times(hosts):
    schema = ["host_response_time"]
    na = spark.createDataFrame([("N/A",)], schema)

    response_times = hosts.selectExpr("host_response_time")
    response_times = response_times.fillna("N/A").union(na).distinct()
    # for some reason data from other columns which have long texts in them
    # overlap to this column or it's just a bad csv, so I've filtered some of them out
    response_times = (
        response_times.filter(~response_times.host_response_time.rlike("^[^a-zA-Z].*"))
        .filter(~response_times.host_response_time.contains("https"))
        .filter(~response_times.host_response_time.contains(","))
        .filter(f.expr("length(host_response_time) <= 60"))
    )
    response_times = response_times.select(
        "*",
        f.row_number()
        .over(Window.orderBy(f.monotonically_increasing_id()))
        .alias("id"),
    )
    response_times = response_times.selectExpr("id", "host_response_time")

    return response_times


def get_ref_property_types(listings):
    schema = ["property_type"]
    na = spark.createDataFrame([("N/A",)], schema)

    property_types = listings.selectExpr("property_type")
    property_types = property_types.fillna("N/A").union(na).distinct()
    # for some reason data from other columns which have long texts in them
    # overlap to this column or it's just a bad csv, so I've filtered some of them out
    property_types = (
        property_types.filter(~property_types.property_type.rlike("^[^a-zA-Z].*"))
        .filter(~property_types.property_type.contains("https"))
        .filter(~property_types.property_type.contains(","))
        .filter(f.expr("length(property_type) <= 60"))
    )
    property_types = property_types.select(
        "*",
        f.row_number()
        .over(Window.orderBy(f.monotonically_increasing_id()))
        .alias("id"),
    )
    property_types = property_types.selectExpr("id", "property_type")

    return property_types


def get_user_ref_countries(reviews):
    schema = ["country"]
    na = spark.createDataFrame([("N/A",)], schema)

    countries = reviews.selectExpr("user_country as country")
    countries = countries.fillna("N/A").union(na).distinct()
    # for some reason data from other columns which have long texts in them
    # overlap to this column or it's just a bad csv, so I've filtered some of them out
    countries = (
        countries.filter(~countries.country.rlike("^[^a-zA-Z].*"))
        .filter(~countries.country.contains("https"))
        .filter(f.expr("length(country) <= 100"))
    )
    countries = countries.select(
        "*",
        f.row_number()
        .over(Window.orderBy(f.monotonically_increasing_id()))
        .alias("id"),
    )
    countries = countries.selectExpr("id", "country")

    return countries


def fill_ref_listing_locations():
    df = get_listing_locations(listings)
    df.write.mode("overwrite").saveAsTable("airbnb_dv.ref_listing_locations")


def fill_ref_countries():
    df = get_user_ref_countries(reviews)
    df.write.mode("overwrite").saveAsTable("airbnb_dv.ref_countries")


def fill_ref_response_times():
    df = get_ref_response_times(hosts)
    df.write.mode("overwrite").saveAsTable("airbnb_dv.ref_response_times")


def fill_ref_property_types():
    df = get_ref_property_types(listings)
    df.write.mode("overwrite").saveAsTable("airbnb_dv.ref_property_types")


def create_refs():
    create_tables()
    fill_ref_response_times()
    fill_ref_property_types()
    fill_ref_listing_locations()
    fill_ref_countries()


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
    reviews_path = "/airflow/data/csv/reviews_8_10_Sep_2022.csv"

    hosts = spark.read.option("header", True).csv(existing_hosts_path)
    listings = spark.read.option("header", True).csv(existing_listings_path)
    reviews = spark.read.option("header", True).csv(reviews_path)
    create_refs()
