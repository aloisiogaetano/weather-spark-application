from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def filter_year_and_hours(
    df: DataFrame,
    year: int,
    start_hour: int = 12,
    end_hour: int = 15
) -> DataFrame:
    """
    Filtra il dataframe per anno e fascia oraria locale.
    """
    return (
        df
        .filter(F.col("year") == year)
        .filter(F.col("hour").between(start_hour, end_hour))
    )
