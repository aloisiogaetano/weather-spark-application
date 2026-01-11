from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def compute_seasonal_avg(df: DataFrame) -> DataFrame:
    """
    Calcola la temperatura media per città e stagione.
    """
    return (
        df
        .groupBy("city", "season")
        .agg(F.avg("temperature").alias("avg_temperature"))
    )
