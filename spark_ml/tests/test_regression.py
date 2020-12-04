from Regression import make_regr_model
import findspark
import os
from time import time
from cassandra.cluster import Cluster
import pytest


# Test sprawdzający czy powstały automatycznie model ma wszystkie wymagane etapy
def test_make_regr_model_pipeline(spark_df_regr):
    spark, df = spark_df_regr
    path = findspark.find()
    path = os.path.join(path, 'models')
    test_path = os.path.join(path, 'test_regr_model')
    test_path = test_path + '_' + str(int(time()))
    model, _ = make_regr_model(df, spark, test_path, "test_regr_model", "test_target",
                                                              save=False)
    actual_stages = str([type(x) for x in model.stages])
    expected_stages = "[<class 'pyspark.ml.feature.StringIndexerModel'>," \
                      " <class 'pyspark.ml.feature.StringIndexerModel'>," \
                      " <class 'pyspark.ml.feature.OneHotEncoderModel'>," \
                      " <class 'pyspark.ml.feature.VectorAssembler'>," \
                      " <class 'pyspark.ml.feature.VectorAssembler'>," \
                      " <class 'pyspark.ml.feature.StandardScalerModel'>," \
                      " <class 'pyspark.ml.feature.VectorAssembler'>," \
                      " <class 'pyspark.ml.regression.RandomForestRegressionModel'>]"
    assert actual_stages == expected_stages


# Test sprawdzający czy model poprawnie zapisuje się do tabeli w bazie danych Cassandra
def test_make_regr_model_saving(spark_df_regr, pandas_factory_fixture):
    spark, df = spark_df_regr
    path = findspark.find()
    path = os.path.join(path, 'models')
    test_path = os.path.join(path, 'test_regr_model')
    test_path = test_path + '_' + str(int(time()))
    make_regr_model(df, spark, test_path, "test_regr_model", "test_target",
                                save=True)
    cluster = Cluster(['127.0.0.1'], "9042")
    session = cluster.connect("models")
    session.row_factory = pandas_factory_fixture
    session.default_fetch_size = None
    query_latest = "Select Max(timestamp) from models_statistics where model_name = 'test_regr_model'"
    query_path = ("Select model_path from models_statistics where timestamp = %s and model_name = 'test_regr_model'"
                  " ALLOW FILTERING")
    latest_timestamp = session.execute(query_latest, timeout=None)._current_rows.iloc[0]['system.max(timestamp)']
    query_path = query_path % latest_timestamp
    path = session.execute(query_path, timeout=None)._current_rows.iloc[0]['model_path']

    expected_path = test_path
    actual_path = path
    assert actual_path == expected_path
