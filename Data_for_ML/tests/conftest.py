from pyspark.sql import SparkSession
import pytest


@pytest.fixture(scope='session')
def spark():
    spark = SparkSession.builder.getOrCreate()
    yield spark
    spark.stop()

