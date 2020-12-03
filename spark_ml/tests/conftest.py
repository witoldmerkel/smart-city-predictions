from pyspark.sql import SparkSession
import pytest


@pytest.fixture(scope='session')
def spark_df_class():
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame([
        ("yes", 1.0, 0.0, 1.1, 0.1, "sun"),
        ("no", 0.0, 2.0, 1.0, -1.0, "moon"),
        ("no", 0.0, 2.0, 1.3, 1.0, "left"),
        ("yes", 1.0, 0.0, 1.2, -0.5, "right")], ["target", "v1", "v2", "v3", "v4", "v5"])
    yield spark, df
    spark.stop()

@pytest.fixture(scope='session')
def spark_df_regr():
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame([
        ("yes", 1.0, 0.0, 1.1, 0.1, "sun"),
        ("no", 0.0, 2.0, 1.0, -1.0, "moon"),
        ("no", 0.0, 2.0, 1.3, 1.0, "left"),
        ("yes", 1.0, 0.0, 1.2, -0.5, "right")], ["v1", "target", "v2", "v3", "v4", "v5"])
    yield spark, df
    spark.stop()
