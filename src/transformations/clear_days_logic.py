from pyspark.sql import DataFrame, functions as F

CLEAR_WEATHER_VALUES = ["sky is clear"]
SPRING_MONTHS = [3, 4, 5]
MIN_CLEAR_HOURS_PER_DAY = 18
MIN_CLEAR_DAYS_PER_MONTH = 15


def find_spring_clear_cities(weather_df: DataFrame) -> DataFrame:

    # -------------------------
    # 1. Parse timestamp
    # -------------------------
    df = (
        weather_df
        .withColumn("timestamp", F.to_timestamp("datetime"))
    )

    # -------------------------
    # 2. Unpivot (wide -> long)
    # -------------------------
    city_columns = [c for c in df.columns if c not in ("datetime", "timestamp")]

    long_df = df.select(
        "timestamp",
        F.expr(
            f"stack({len(city_columns)}, "
            + ", ".join([f"'{c}', `{c}`" for c in city_columns])
            + ") as (city, weather_description)"
        )
    )

    # -------------------------
    # 3. Filter valid records
    # -------------------------
    filtered = (
        long_df
        .filter(F.col("weather_description").isNotNull())
        .filter(F.col("weather_description").isin(CLEAR_WEATHER_VALUES))
        .withColumn("date", F.to_date("timestamp"))
        .withColumn("year", F.year("timestamp"))
        .withColumn("month", F.month("timestamp"))
        .filter(F.col("month").isin(SPRING_MONTHS))
    )

    # -------------------------
    # 4. Clear hours per day
    # -------------------------
    daily_clear_hours = (
        filtered
        .groupBy("city", "year", "month", "date")
        .count()
        .withColumnRenamed("count", "clear_hours")
    )

    # -------------------------
    # 5. Clear days
    # -------------------------
    clear_days = (
        daily_clear_hours
        .filter(F.col("clear_hours") >= MIN_CLEAR_HOURS_PER_DAY)
    )

    # -------------------------
    # 6. Clear days per month
    # -------------------------
    monthly_clear_days = (
        clear_days
        .groupBy("city", "year", "month")
        .count()
        .withColumnRenamed("count", "clear_days")
        .filter(F.col("clear_days") >= MIN_CLEAR_DAYS_PER_MONTH)
    )

    # -------------------------
    # 7. AND condition on months
    # -------------------------
    result = (
        monthly_clear_days
        .groupBy("city", "year")
        .agg(F.countDistinct("month").alias("valid_months"))
        .filter(F.col("valid_months") == len(SPRING_MONTHS))
        .select("year", "city")
        .orderBy("year", "city")
    )

    return result
