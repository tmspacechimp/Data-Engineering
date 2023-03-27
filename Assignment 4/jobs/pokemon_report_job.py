from pyspark.sql.functions import when, col, countDistinct, avg
from pyspark.sql.types import IntegerType

from jobs import spark_utils, hdfs_utils

if __name__ == '__main__':
    spark = spark_utils.session_init()

    path = hdfs_utils.namenode + "/user/hive/warehouse/staging/filteredData"
    df = spark.read.option("header", True).parquet(path)

    # is_legendary
    df = df.withColumn("is_legendary",
                       col("is_legendary").cast(IntegerType()))
    grouped_df = df.groupBy("type").sum("is_legendary")

    # abilities
    grouped_df = df.groupBy("type").agg(countDistinct("abilities"))\
        .join(grouped_df)

    # against sums
    against_cols = ["against_bug", "against_dark", "against_dragon",
                    "against_electric", "against_fairy", "against_fight",
                    "against_fire", "against_flying", "against_ghost",
                    "against_grass", "against_ground", "against_ice", "against_normal",
                    "against_poison", "against_psychic", "against_rock",
                    "against_steel", "against_water"]

    for field_name in against_cols:
        grouped_df = df.groupBy("type").agg(
            sum(field_name).alias("sum_" + field_name)
        ).join(grouped_df)

    # avg, min, max on abilities

    abilities_cols = ["attack", "base_egg_steps", "base_happiness",
                      "base_total", "capture_rate", "defense", "height_m",
                      "hp", "sp_attack", "sp_defense", "speed", "weight_kg"]

    for field_name in abilities_cols:
        grouped_df = df.groupBy("type").agg(
            avg(field_name).alias("avg_" + field_name),
            min(field_name).alias("min_" + field_name),
            max(field_name).alias("max_" + field_name)
        ).join(grouped_df)






