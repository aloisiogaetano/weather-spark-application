from src.transformations.add_season import add_season


def test_add_season_assignment(spark):
    rows = [
        ("Rome", 1),   # Jan
        ("Rome", 4),   # Apr
        ("Rome", 6),   # Jun
        ("Rome", 9),   # Sep
        ("Rome", 12),  # Dec (ignored)
    ]

    df = spark.createDataFrame(rows, ["city", "month"])

    result = add_season(df).select("month", "season").collect()

    mapping = {r["month"]: r["season"] for r in result}

    assert mapping[1] == "cold"
    assert mapping[4] == "cold"
    assert mapping[6] == "hot"
    assert mapping[9] == "hot"
    assert 12 not in mapping
