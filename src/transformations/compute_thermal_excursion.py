from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def compute_thermal_excursion(df: DataFrame) -> DataFrame:
    """
    Calcola l'escursione termica stagionale per città.
    """
    pivoted = (
        df
        .groupBy("city")
        .pivot("season", ["cold", "hot"])
        .agg(F.first("avg_temperature"))
    )

    return pivoted.withColumn(
        "thermal_excursion",
        F.col("hot") - F.col("cold")
    )
