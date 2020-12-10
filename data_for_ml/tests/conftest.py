from pyspark.sql import SparkSession
import pytest
import os


# Plik zawierający kod wielokrotnie wykorzystywany podczas testów dla tego modułu
@pytest.fixture(scope='session')
def spark():
    os.environ[
        'PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.0.0' \
                                 ' --conf spark.cassandra.connection.host=127.0.0.1 pyspark-shell'
    spark = SparkSession.builder.getOrCreate()
    yield spark
    spark.stop()

