import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def mock_views_df():
    # TODO: Pull this out so it only executes once
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("tests")
        .config("spark.ui.enabled", False)
        .getOrCreate()
    )
    return spark.read.json("./tests/sample-data.json")
