from pyspark.sql import DataFrame
from pyspark.sql import functions as F
#from transformations.timezone_utils import CITY_TIMEZONE

CITY_TIMEZONE = {
    "Phoenix": "America/Phoenix",
    "Nahariyya": "Asia/Jerusalem",
    "Dallas": "America/Chicago",
    "San Antonio": "America/Chicago",
    "Philadelphia": "America/New_York",
    "Los Angeles": "America/Los_Angeles",
    "Indianapolis": "America/Indiana/Indianapolis",
    "San Francisco": "America/Los_Angeles",
    "San Diego": "America/Los_Angeles",
    "Nashville": "America/Chicago",
    "Detroit": "America/Detroit",
    "Portland": "America/Los_Angeles",
    "Haifa": "Asia/Jerusalem",
    "Montreal": "America/Montreal",
    "Jerusalem": "Asia/Jerusalem",
    "Pittsburgh": "America/New_York",
    "Beersheba": "Asia/Jerusalem",
    "Chicago": "America/Chicago",
    "Toronto": "America/Toronto",
    "Vancouver": "America/Vancouver",
    "Tel Aviv District": "Asia/Jerusalem",
    "Atlanta": "America/New_York",
    "Las Vegas": "America/Los_Angeles",
    "Seattle": "America/Los_Angeles",
    "Kansas City": "America/Chicago",
    "Saint Louis": "America/Chicago",
    "Minneapolis": "America/Chicago",
    "Houston": "America/Chicago",
    "Jacksonville": "America/New_York",
    "Albuquerque": "America/Denver",
    "Miami": "America/New_York",
    "New York": "America/New_York",
    "Charlotte": "America/New_York",
    "Eilat": "Asia/Jerusalem",
    "Denver": "America/Denver",
    "Boston": "America/New_York"
}

def convert_datetime_to_local(df: DataFrame, datetime_col: str = "datetime") -> DataFrame:
    """
    Add local datetime, year, month columns based on city timezone mapping.
    """
    # aggiunge la colonna timezone
    df = df.withColumn(
        "timezone",
        F.create_map(
            *[F.lit(x) for x in sum(CITY_TIMEZONE.items(), ())]
        )[F.col("city")]
    )

    # converte UTC → locale
    df = df.withColumn("datetime_local", F.from_utc_timestamp(F.col(datetime_col), F.col("timezone")))

    # estrae year e month
    df = df.withColumn("year", F.year("datetime_local"))
    df = df.withColumn("month", F.month("datetime_local"))
    df = df.withColumn("hour", F.hour("datetime_local"))
    return df
