from utils.spark_session import get_spark
from transformations.wide_to_long import reshape_wide_to_long
from transformations.join_metrics import join_metrics
from transformations.convert_to_local import convert_datetime_to_local
from transformations.aggregate_nation_stats import aggregate_nation_stats

import os
from pyspark.sql import functions as F


def run(temperature_path: str, pressure_path: str, humidity_path: str, city_attributes_path: str ):
    """
    Task 2: Statistiche meteorologiche per nazione.
    """

    spark = get_spark("task-2-nation-stats")


    # Leggi CSV wide
    temp_df = spark.read.csv(temperature_path, header=True, inferSchema=True)
    pressure_df = spark.read.csv(pressure_path, header=True, inferSchema=True)
    humidity_df = spark.read.csv(humidity_path, header=True, inferSchema=True)
    city_attr_df = spark.read.csv(city_attributes_path, header=True, inferSchema=True)


    temp_long = reshape_wide_to_long(temp_df)
    pressure_long = reshape_wide_to_long(pressure_df)
    humidity_long = reshape_wide_to_long(humidity_df)

    pressure_long= pressure_long.withColumnRenamed("value", "pressure")
    temp_long= temp_long.withColumnRenamed("value", "temperature")
    humidity_long= humidity_long.withColumnRenamed("value", "humidity")

    

    metrics_df = join_metrics([
    temp_long,
    pressure_long,
    humidity_long
])

    metrics_df = convert_datetime_to_local(metrics_df)

    # Aggrega per nazione/mese/anno
    nation_stats_df = aggregate_nation_stats(metrics_df, city_attr_df)

    nation_stats_df = nation_stats_df.orderBy(
        F.col("country"),
        F.col("year"),
        F.col("month")
    )

    # Mostra risultato
    nation_stats_df.show(200, truncate=False)

    spark.stop()
