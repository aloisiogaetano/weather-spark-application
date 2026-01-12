from pyspark.sql import DataFrame, functions as F
from transformations.wide_to_long import reshape_wide_to_long

CLEAR_WEATHER_VALUES = ["sky is clear"]
SPRING_MONTHS = [3, 4, 5]
MIN_CLEAR_HOURS_PER_DAY = 18
MIN_CLEAR_DAYS_PER_MONTH = 15


def find_spring_clear_cities(weather_df: DataFrame) -> DataFrame:

   
    # -------------------------
    # 1. Unpivot (wide -> long)
    # -------------------------
   
    long_df = reshape_wide_to_long(
        weather_df,value_col_name="weather_description").withColumn("timestamp", F.to_timestamp("datetime"))
    # -------------------------
    # 2. Filter valid records
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
    # 3. Clear hours per day
    # -------------------------
    daily_clear_hours = (
        filtered
        .groupBy("city", "year", "month", "date")
        .count()
        .withColumnRenamed("count", "clear_hours")
    )

    # -------------------------
    # 4. Clear days
    # -------------------------
    clear_days = (
        daily_clear_hours
        .filter(F.col("clear_hours") >= MIN_CLEAR_HOURS_PER_DAY)
    )

    # -------------------------
    # 5. Clear days per month
    # -------------------------
    monthly_clear_days = (
        clear_days
        .groupBy("city", "year", "month")
        .count()
        .withColumnRenamed("count", "clear_days")
        .filter(F.col("clear_days") >= MIN_CLEAR_DAYS_PER_MONTH)
    )

    # -------------------------
    # 6. AND condition on months
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
