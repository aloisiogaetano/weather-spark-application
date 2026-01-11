from pyspark.sql import DataFrame
from functools import reduce


def join_metrics(
    dfs: list[DataFrame],
    join_cols: list[str] = ["datetime", "city"]
) -> DataFrame:
    """
    Join multiple metric DataFrames on (datetime, city) using FULL OUTER JOIN.

    Expected schema for each df:
        datetime | city | <metric>

    Returns:
        datetime | city | metric1 | metric2 | ...
    """

    if not dfs:
        raise ValueError("No DataFrames provided for join")

    def _join(left: DataFrame, right: DataFrame) -> DataFrame:
        return left.join(right, on=join_cols, how="full")

    return reduce(_join, dfs)
