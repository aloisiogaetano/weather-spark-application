from src.transformations.clear_days_logic import find_spring_clear_cities
from pyspark.sql.types import StructType, StructField, StringType


def test_city_with_all_clear_spring(spark):
    """
    Rome:
    - March, April, May
    - Every day has >=18 clear hours
    """
    rows = []

    # generate 15 clear days per month, 18 hours per day
    for month in [3, 4, 5]:
        for day in range(1, 16):
            for hour in range(18):
                rows.append(
                    (f"2021-{month:02d}-{day:02d} {hour:02d}:00:00", "sky is clear", None)
                )
    
    schema = StructType([
    StructField("datetime", StringType(), True),
    StructField("Rome", StringType(), True),
    StructField("Paris", StringType(), True),
    ])

    df = spark.createDataFrame(
        rows,
        schema=schema
    )

    result = find_spring_clear_cities(df).collect()

    assert len(result) == 1
    assert result[0]["city"] == "Rome"
    assert result[0]["year"] == 2021


def test_city_missing_one_spring_month(spark):
    """
    Paris:
    - Clear in March and April
    - Missing May
    """
    rows = []

    for month in [3, 4]:
        for day in range(1, 16):
            for hour in range(18):
                rows.append(
                    (f"2021-{month:02d}-{day:02d} {hour:02d}:00:00", None, "sky is clear")
                )
    
    schema = StructType([
    StructField("datetime", StringType(), True),
    StructField("Rome", StringType(), True),
    StructField("Paris", StringType(), True),
    ])

    df = spark.createDataFrame(
        rows,
        schema=schema
    )

    result = find_spring_clear_cities(df).collect()

    cities = {r["city"] for r in result}
    assert "Paris" not in cities


def test_ignore_non_clear_weather(spark):
    """
    City with mixed weather should not pass
    """
    rows = []

    for day in range(1, 16):
        for hour in range(18):
            rows.append(
                ("2021-03-%02d %02d:00:00" % (day, hour), "cloudy", None)
            )
    schema = StructType([
    StructField("datetime", StringType(), True),
    StructField("Rome", StringType(), True),
    StructField("Paris", StringType(), True),
    ])
    df = spark.createDataFrame(
        rows,
        schema=schema
    )

    result = find_spring_clear_cities(df).collect()

    assert result == []
