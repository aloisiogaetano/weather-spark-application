from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from src.transformations.join_metrics import join_metrics


def test_join_metrics_full_outer(spark):
    temp_schema = StructType([
        StructField("datetime", StringType(), True),
        StructField("city", StringType(), True),
        StructField("temperature", DoubleType(), True),
    ])

    pressure_schema = StructType([
        StructField("datetime", StringType(), True),
        StructField("city", StringType(), True),
        StructField("pressure", DoubleType(), True),
    ])

    temp_rows = [
        ("2021-01-01 00:00:00", "Rome", 280.0),
    ]

    pressure_rows = [
        ("2021-01-01 00:00:00", "Rome", 1013.0),
        ("2021-01-01 00:00:00", "Paris", 1010.0),
    ]

    temp_df = spark.createDataFrame(temp_rows, temp_schema)
    pressure_df = spark.createDataFrame(pressure_rows, pressure_schema)

    result = join_metrics([temp_df, pressure_df]).collect()

    assert len(result) == 2
    rome = next(r for r in result if r.city == "Rome")
    paris = next(r for r in result if r.city == "Paris")

    assert rome.temperature == 280.0
    assert rome.pressure == 1013.0
    assert paris.temperature is None
    assert paris.pressure == 1010.0
