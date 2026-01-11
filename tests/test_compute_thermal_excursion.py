from src.transformations.compute_thermal_excursion import compute_thermal_excursion


def test_compute_thermal_excursion(spark):
    rows = [
        ("Rome", "hot", 30.0),
        ("Rome", "cold", 10.0),
    ]

    df = spark.createDataFrame(
        rows,
        ["city", "season", "avg_temperature"]
    )

    result = compute_thermal_excursion(df).collect()

    assert len(result) == 1
    assert result[0]["thermal_excursion"] == 20.0
