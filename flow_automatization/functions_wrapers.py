import findspark
import spark_ml.classificator.Classification
import spark_ml.reggresor.Regression
import data_for_ml.powietrze_manipulation
import data_for_ml.urzedy_manipulation
import data_for_ml.velib_manipulation
import speed_layer.speed_connection
import pandas as pd
from cassandra.cluster import Cluster
import tempfile
import itertools as IT
from time import time
import platform
import os
# W tym pliku definujemy procesy ładowania danych z głównego zbioru danych oraz uczenie modeli dla róźnych zbiorów danych
#dla których zdefiniowane są funkcje wstępnego przetwarzania oraz ładowania tabeli


def load_and_train(source):
    plt = platform.system()
    if plt == "Linux":
        findspark.init("/home/smartcity/Downloads/spark-3.0.1-bin-hadoop2.7")
    path = findspark.find()
    path = os.path.join(path, 'models')

    if source == "powietrze":
        # Załadowanie tabeli powietrze z bazy danych master dataset
        data_pow, sc_pow = data_for_ml.powietrze_manipulation.load_powietrze()

        # Wytrenowanie modelu klsyfikacyjnego na wcześniej załadowanych danych

        powietrze_path = os.path.join(path, 'powietrze_model')
        powietrze_path = powietrze_path + '_' + str(int(time()))
        spark_ml.classificator.Classification.make_class_model(data_pow, sc_pow, powietrze_path, 'RF_pow', 'pm25')

    elif source == "urzedy":
        data_urz, sc_urz = data_for_ml.urzedy_manipulation.load_urzedy(agg="moving_average")

        urzedy_path = os.path.join(path, 'urzedy_model')
        urzedy_path = urzedy_path + '_' + str(int(time()))
        spark_ml.reggresor.Regression.make_regr_model(data_urz, sc_urz, urzedy_path, 'RF_urz_mav', "liczbaKlwKolejce")

    elif source == "velib":
        data_vel, sc_vel = data_for_ml.velib_manipulation.load_velib()

        velib_path = os.path.join(path, 'velib_model')
        velib_path = velib_path + '_' + str(int(time()))
        spark_ml.reggresor.Regression.make_regr_model(data_vel, sc_vel, velib_path, 'RF_vel', 'numbikesavailable')


def activate_stream(source, spark, sk_connection):

    if source == "powietrze":

        powietrze_path = get_best_model_path("'RF_pow'", 'max')
        # Uruchomienia modułu szybkiego przetwarzania dla powietrza, który korzysta z wcześniej nauczonych modeli
        query, _ = speed_layer.speed_connection.activate_powietrze_stream(model_path=powietrze_path, spark=spark,
                                                                         sk_connection=sk_connection)

    elif source == "urzedy":
        urzedy_path = get_best_model_path("'RF_urz_mav'", 'min')
        print(urzedy_path)
        query, _ = speed_layer.speed_connection.activate_urzedy_stream(model_path=urzedy_path, spark=spark,
                                                                         sk_connection=sk_connection,
                                                                       agg="moving_average")

    elif source == "velib":
        velib_path = get_best_model_path("'RF_vel'", 'min')
        query, _ = speed_layer.speed_connection.activate_velib_stream(model_path=velib_path, spark=spark,
                                                                         sk_connection=sk_connection)

    return query

# Factory dla wczytywania danych z Cassandry


def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)

# Funkcja która zwraca ścieżkę do aktulanie najlepszego modelu pod względem stat dla wybranych modeli


def get_best_model_path(model_name, stat):
    # Szukanie modelu z najlepszą skutecznością
    cluster = Cluster(['127.0.0.1'], "9042")
    session = cluster.connect("models")
    session.row_factory = pandas_factory
    session.default_fetch_size = None
    if stat == 'max':
        query_max = ("Select Max(stat) from models_statistics where model_name = %s")
    else:
        query_max = ("Select Min(stat) from models_statistics where model_name = %s")
    query_path = ("Select model_path from models_statistics where stat = %s and model_name = %s ALLOW FILTERING")
    query_max = query_max % model_name
    if stat == 'max':
        max_stat = session.execute(query_max, timeout=None)._current_rows.iloc[0]['system.max(stat)']
    else:
        max_stat = session.execute(query_max, timeout=None)._current_rows.iloc[0]['system.min(stat)']
    query_path = query_path % (max_stat, model_name)
    path = session.execute(query_path, timeout=None)._current_rows.iloc[0]['model_path']
    session.shutdown()
    cluster.shutdown()
    return path

# Funkcja zapewniająca unikalność nazw folderów dla modeli


def uniquify(path, sep=''):
    def name_sequence():
        count = IT.count()
        yield ''
        while True:
            yield '{s}{n:d}'.format(s=sep, n=next(count))
    orig = tempfile._name_sequence
    with tempfile._once_lock:
        tempfile._name_sequence = name_sequence()
        path = os.path.normpath(path)
        dirname, basename = os.path.split(path)
        filename, ext = os.path.splitext(basename)
        fd, filename = tempfile.mkstemp(dir=dirname, prefix=filename, suffix=ext)
        tempfile._name_sequence = orig
    return filename

