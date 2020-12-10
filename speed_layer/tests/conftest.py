from pyspark.sql import SparkSession
import pytest
from streams_handling import stream_to_predictions
from cassandra.cluster import Cluster
import pandas as pd
import pyspark.sql.functions as F
from time import time
import os


# Plik zawierający kod wielokrotnie wykorzystywany podczas testów dla tego modułu
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
    os.environ[
        'PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.0.0' \
                                 ' --conf spark.cassandra.connection.host=127.0.0.1 pyspark-shell'
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
    yield df, spark
    spark.stop()


@pytest.fixture()
def predictions(stream, model_path):
    timestamp = int(time())
    stream, spark = stream
    predictions = stream_to_predictions(stream, model_path, 'test_target', 'test_source')
    predictions = predictions.withColumn('individual', predictions['v5'])
    predictions = predictions.withColumn("timestamp", F.lit(timestamp))
    predictions = predictions.select('prediction', 'individual', "source_name", "timestamp", "target_column",
                                     "model_path")
    return predictions, spark, timestamp
