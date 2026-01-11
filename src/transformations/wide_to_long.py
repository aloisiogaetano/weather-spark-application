from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def reshape_wide_to_long(
    df: DataFrame,
    datetime_col: str = "datetime",
    value_col_name: str = "value"
) -> DataFrame:
    """
    Convert a wide Spark DataFrame:
        datetime | city1 | city2 | city3 | ...

    Into a long format:
        datetime | city | value

    - Ignores the datetime column
    - Keeps null values (filtered later if needed)
    """

    city_columns = [c for c in df.columns if c != datetime_col]

    if not city_columns:
        raise ValueError("No city columns found to reshape")

    # build stack expression
    # stack(n, 'city1', city1, 'city2', city2, ...)
    stack_expr = ", ".join(
        [f"'{c}', `{c}`" for c in city_columns]
    )

    return (
        df.selectExpr(
            f"`{datetime_col}`",
            f"stack({len(city_columns)}, {stack_expr}) as (city, {value_col_name})"
        )
    )
