from pyspark.sql.types import StructType, StructField, StringType
from src.transformations.wide_to_long import reshape_wide_to_long


def test_wide_to_long_basic(spark):
    schema = StructType([
        StructField("datetime", StringType(), True),
        StructField("Rome", StringType(), True),
        StructField("Paris", StringType(), True),
    ])

    rows = [
        ("2021-01-01 00:00:00", "10", "20"),
    ]

    df = spark.createDataFrame(rows, schema)
    result = reshape_wide_to_long(df).collect()

    assert len(result) == 2
    cities = {r["city"] for r in result}
    assert cities == {"Rome", "Paris"}
