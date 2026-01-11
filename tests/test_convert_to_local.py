from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from src.transformations.convert_to_local import convert_datetime_to_local


def test_convert_datetime_to_local(spark):
    schema = StructType([
        StructField("datetime", StringType(), True),
        StructField("city", StringType(), True),
        StructField("temperature", DoubleType(), True),
    ])

    rows = [
        ("2021-03-01 12:00:00", "New York", 275.0),
        ("2021-03-01 12:00:00", "San Francisco", 280.0),
    ]

    df = spark.createDataFrame(rows, schema)
    df_local = convert_datetime_to_local(df)

    result = {r["city"]: (r["datetime_local"], r["year"], r["month"]) for r in df_local.collect()}

    assert result["New York"][1] == 2021
    assert result["New York"][2] == 3
    assert result["San Francisco"][1] == 2021
    assert result["San Francisco"][2] == 3
