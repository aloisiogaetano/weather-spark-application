from src.transformations.rank_cities_by_country import rank_cities_by_country


def test_rank_cities_by_country(spark):
    rows = [
        ("Rome", "Italy", 25.0),
        ("Milan", "Italy", 20.0),
        ("Naples", "Italy", 15.0),
        ("Turin", "Italy", 10.0),
    ]

    df = spark.createDataFrame(
        rows,
        ["city", "country", "thermal_excursion"]
    )

    result = rank_cities_by_country(df, top_n=3).collect()

    cities = [r["city"] for r in result]

    assert cities == ["Rome", "Milan", "Naples"]
