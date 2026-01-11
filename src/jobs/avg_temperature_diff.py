from utils.spark_session import get_spark
from transformations.wide_to_long import reshape_wide_to_long
from transformations.convert_to_local import convert_datetime_to_local
from transformations.filter_year_and_hours import filter_year_and_hours
from transformations.add_season import add_season
from transformations.compute_thermal_excursion import compute_thermal_excursion
from transformations.rank_cities_by_country import rank_cities_by_country
from transformations.compute_seasonal_avg  import compute_seasonal_avg


from pyspark.sql import functions as F


def run(temperature_path: str, city_attributes_path: str):
    """
    Task 3: escursione termica media.
    """

    spark = get_spark("task-3-nation-stats")
    temp_df = spark.read.csv(temperature_path, header=True, inferSchema=True)
    city_attr_df = spark.read.csv(city_attributes_path, header=True, inferSchema=True)

    temp_long = reshape_wide_to_long(temp_df).withColumnRenamed("value", "temperature")
    temp_local = convert_datetime_to_local(temp_long)


    def compute_for_year(year: int):
        df = (
            temp_local
            .transform(lambda d: filter_year_and_hours(d, year))
            .transform(add_season)
            .transform(compute_seasonal_avg)
            .transform(compute_thermal_excursion)
        )

        # Join con country
        df = (
            df.join(
                city_attr_df.select("city", "country"),
                on="city",
                how="left"
            )
        )

        # Ranking top 3
        return rank_cities_by_country(df, top_n=3).withColumnRenamed(
            "rank", f"rank_{year}"
        )
    
    
    # Calcolo 2017 e 2016
   
    stats_2017 = compute_for_year(2017)
    stats_2016 = compute_for_year(2016).select("city", "country", "rank_2016")
    stats_2016.show()
    

    
    # Confronto ranking
    
    result = (
        stats_2017
        .join(
            stats_2016,
            on=["city", "country"],
            how="left"
        )
        .orderBy("country", "rank_2017")
    )

    result.show(200,truncate=False)

    spark.stop()






    