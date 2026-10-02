import pytest
from pyspark.sql import SparkSession

from pyspark_job import clean_data


@pytest.fixture(scope="session")
def spark():
    spark = (
    SparkSession.builder
    .master("local[1]")
    .appName("PySparkTest")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
    )

    yield spark

    spark.stop()


@pytest.fixture
def sample_data(spark):
    data = [
        (1, "Alice", 100.0),
        (2, "Bob", 50.0),
        (3, "Charlie", 0.0),
        (4, "David", -20.0),
        (5, None, 80.0),
    ]

    return spark.createDataFrame(
        data,
        ["id", "name", "amount"]
    )


def test_valid_records_are_kept(sample_data):
    result = clean_data(sample_data)

    rows = result.collect()

    assert len(rows) == 2

    names = {row["name"] for row in rows}

    assert names == {"Alice", "Bob"}


def test_amount_less_than_or_equal_to_zero_removed(sample_data):
    result = clean_data(sample_data)

    amounts = [row["amount"] for row in result.collect()]

    assert all(amount > 0 for amount in amounts)


def test_null_names_are_removed(sample_data):
    result = clean_data(sample_data)

    names = [row["name"] for row in result.collect()]

    assert None not in names


def test_amount_with_tax_calculated_correctly(sample_data):
    result = clean_data(sample_data)

    rows = {
        row["name"]: row["amount_with_tax"]
        for row in result.collect()
    }

    assert rows["Alice"] == pytest.approx(120.0)
    assert rows["Bob"] == pytest.approx(60.0)