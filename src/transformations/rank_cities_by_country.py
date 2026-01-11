from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def rank_cities_by_country(df: DataFrame, top_n: int = 3) -> DataFrame:
    """
    Restituisce le top N città per escursione termica per nazione.
    """
    window = Window.partitionBy("country").orderBy(F.desc("thermal_excursion"))

    return (
        df
        .withColumn("rank", F.row_number().over(window))
        .filter(F.col("rank") <= top_n)
    )
