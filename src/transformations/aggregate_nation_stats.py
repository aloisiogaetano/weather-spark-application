from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def aggregate_nation_stats(metrics_df: DataFrame, city_attr_df: DataFrame) -> DataFrame:
    """
    Aggrega temperature, pressure, humidity per nazione e mese/anno.

    Args:
        metrics_df: df con colonne ['city', 'datetime_local', 'year', 'month', 'temperature', 'pressure', 'humidity']
        city_attr_df: df con colonne ['city', 'country']

    Returns:
        DataFrame con schema:
        ['country', 'year', 'month',
         'avg_temperature', 'std_temperature', 'min_temperature', 'max_temperature',
         'avg_pressure', 'std_pressure', 'min_pressure', 'max_pressure',
         'avg_humidity', 'std_humidity', 'min_humidity', 'max_humidity']
    """

    # Join città → nazione
    df = metrics_df.join(city_attr_df.select("city", "country"), on="city", how="left")

    # Raggruppa per nazione + anno + mese
    agg_df = df.groupBy("country", "year", "month").agg(
        F.avg("temperature").alias("avg_temperature"),
        F.stddev("temperature").alias("std_temperature"),
        F.min("temperature").alias("min_temperature"),
        F.max("temperature").alias("max_temperature"),

        F.avg("pressure").alias("avg_pressure"),
        F.stddev("pressure").alias("std_pressure"),
        F.min("pressure").alias("min_pressure"),
        F.max("pressure").alias("max_pressure"),

        F.avg("humidity").alias("avg_humidity"),
        F.stddev("humidity").alias("std_humidity"),
        F.min("humidity").alias("min_humidity"),
        F.max("humidity").alias("max_humidity")
    )

    return agg_df
