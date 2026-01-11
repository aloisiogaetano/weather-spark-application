from utils.spark_session import get_spark
from transformations.clear_days_logic import find_spring_clear_cities


def run(input_path: str):
    spark = get_spark("task-1-clear-days")

    weather_df = spark.read.csv(input_path, header=True, inferSchema=True)
    

    result = find_spring_clear_cities(weather_df)
    result.show(200,truncate=False)

    spark.stop()