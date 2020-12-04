from pyspark.sql import SparkSession
import pytest
from cassandra.cluster import Cluster
import pandas as pd


@pytest.fixture()
def model_path(pandas_factory_fixture):
    cluster = Cluster(['127.0.0.1'], "9042")
    session = cluster.connect("models")
    session.row_factory = pandas_factory_fixture
    session.default_fetch_size = None
    query_latest = "Select Max(timestamp) from models_statistics where model_name = 'test_class_model'"
    query_path = ("Select model_path from models_statistics where timestamp = %s and model_name = 'test_class_model'"
                  " ALLOW FILTERING")
    latest_timestamp = session.execute(query_latest, timeout=None)._current_rows.iloc[0]['system.max(timestamp)']
    query_path = query_path % latest_timestamp
    path = session.execute(query_path, timeout=None)._current_rows.iloc[0]['model_path']
    session.shutdown()
    cluster.shutdown()
    return path


@pytest.fixture(scope='session')
def pandas_factory_fixture():
    def pandas_factory(colnames, rows):
        return pd.DataFrame(rows, columns=colnames)
    return pandas_factory


@pytest.fixture()
def stream():
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
    yield df
    spark.stop()
