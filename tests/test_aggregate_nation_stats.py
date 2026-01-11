from pyspark.sql import Row
from src.transformations.aggregate_nation_stats import aggregate_nation_stats
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

def test_aggregate_nation_stats(spark):
    metrics_rows = [
        ("Rome", 2021, 3, 280.0, 1012.0, 60.0),
        ("Rome", 2021, 3, 282.0, 1015.0, 65.0),
        ("Paris", 2021, 3, 278.0, 1010.0, 70.0)
    ]

    metrics_schema = StructType([
        StructField("city", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("pressure", DoubleType(), True),
        StructField("humidity", DoubleType(), True)
    ])

    metrics_df = spark.createDataFrame(metrics_rows, metrics_schema)

    city_attr_df = spark.createDataFrame([
        ("Rome", "Italy"),
        ("Paris", "France")
    ], ["city", "country"])

    result = aggregate_nation_stats(metrics_df, city_attr_df).collect()

    italy_row = next(r for r in result if r["country"] == "Italy")
    france_row = next(r for r in result if r["country"] == "France")

    assert round(italy_row["avg_temperature"], 1) == 281.0
    assert italy_row["min_pressure"] == 1012.0
    assert france_row["max_humidity"] == 70.0
