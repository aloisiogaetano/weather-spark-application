from src.transformations.compute_seasonal_avg import compute_seasonal_avg


def test_compute_seasonal_avg(spark):
    rows = [
        ("Rome", "hot", 30.0),
        ("Rome", "hot", 32.0),
        ("Rome", "cold", 10.0),
        ("Rome", "cold", 14.0),
    ]

    df = spark.createDataFrame(
        rows,
        ["city", "season", "temperature"]
    )

    result = compute_seasonal_avg(df).collect()

    values = {(r["city"], r["season"]): r["avg_temperature"] for r in result}

    assert values[("Rome", "hot")] == 31.0
    assert values[("Rome", "cold")] == 12.0
