from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_season(df: DataFrame) -> DataFrame:
    """
    Aggiunge la colonna 'season':
    - cold: gennaio-aprile
    - hot: giugno-settembre
    Esclude gli altri mesi.
    """
    return (
        df
        .withColumn(
            "season",
            F.when(F.col("month").between(1, 4), F.lit("cold"))
             .when(F.col("month").between(6, 9), F.lit("hot"))
        )
        .filter(F.col("season").isNotNull())
    )
