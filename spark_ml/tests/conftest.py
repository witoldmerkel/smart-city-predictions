from pyspark.sql import SparkSession
import pandas as pd
import pytest


# Plik zawierający kod wielokrotnie wykorzystywany podczas testów dla tego modułu
@pytest.fixture()
def spark_df_class():
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame([
        ("yes", 1.0, 0.0, 1.1, 0.1, "sun"),
        ("no", 0.0, 2.0, 1.0, -1.0, "moon"),
        ("picture", 0.6, 13.0, 1.0, -1.0, "fast"),
        ("yes", 0.0, 2.0, 1.0, -1.0, "easy"),
        ("nice", 0.2, 2.0, 2.0, -1.0, "good"),
        ("no", 0.0, 2.0, 1.0, -1.0, "off"),
        ("ok", 0.5, 2.0, 1.0, -1.0, "on"),
        ("no", 0.0, 2.0, 1.3, 4.0, "left"),
        ("yes", 1.0, 0.0, 1.2, -0.5, "right")], ["target", "v1", "v2", "v3", "v4", "v5"])
    yield spark, df
    spark.stop()


@pytest.fixture()
def spark_df_regr():
    spark = SparkSession.builder.getOrCreate()
    spark = spark.newSession()
    df = spark.createDataFrame([
        ("yes", 1.0, 0.0, 1.1, 0.1, "sun"),
        ("but", 0.0, 2.0, 1.0, -1.0, "moon"),
        ("picture", 0.6, 13.0, 1.0, -1.0, "fast"),
        ("yes", 0.0, 2.0, 1.0, -1.0, "easy"),
        ("nice", 0.2, 2.0, 2.0, -1.0, "good"),
        ("no", 0.0, 2.0, 1.0, -1.0, "off"),
        ("ok", 0.5, 2.0, 1.0, -1.0, "on"),
        ("no", 0.0, 2.0, 1.3, 1.0, "left"),
        ("or", 1.0, 0.0, 1.2, -0.5, "right")], ["v1", "target", "v2", "v3", "v4", "v5"])
    yield spark, df
    spark.stop()


@pytest.fixture(scope='session')
def pandas_factory_fixture():
    def pandas_factory(colnames, rows):
        return pd.DataFrame(rows, columns=colnames)
    return pandas_factory
