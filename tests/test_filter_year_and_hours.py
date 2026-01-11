from src.transformations.filter_year_and_hours import filter_year_and_hours


def test_filter_year_and_hours(spark):
    rows = [
        ("Rome", "2017-06-01 12:00:00",2017,12, 20.0),
        ("Rome", "2017-06-01 14:00:00",2017,14, 22.0),
        ("Rome", "2017-06-01 16:00:00",2017,16, 30.0),  # fuori fascia
        ("Rome", "2016-06-01 13:00:00",2016,13, 18.0),  # altro anno
    ]

    df = spark.createDataFrame(rows, ["city", "datetime_local","year","hour", "temperature"])

    result = filter_year_and_hours(df, 2017)

    collected = result.collect()
    assert len(collected) == 2

    hours = {r["hour"] for r in collected}
    assert hours == {12, 14}
