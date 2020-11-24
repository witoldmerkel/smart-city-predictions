import os
import findspark
import spark_ml.classificator.Classification
import spark_ml.reggresor.Regression
import Data_for_ML.powietrze_manipulation
import Data_for_ML.urzedy_manipulation
import Data_for_ML.velib_manipulation
import SpeedLayer.speed_connection
#W tym pliku definujemy procesy ładowania danych z głównego zbioru danych oraz uczenie modeli dla róźnych zbiorów danych
#dla których zdefiniowane są funkcje wstępnego przetwarzania oraz ładowania tabeli


def load_and_train(source):
    path = findspark.find()
    if source == "powietrze":
        # Załadowanie tabeli powietrze z bazy danych master dataset
        data_pow, sc_pow = Data_for_ML.powietrze_manipulation.load_powietrze()

        # Wytrenowanie modelu klsyfikacyjnego na wcześniej załadowanych danych
        powietrze_path = os.path.join(path, 'powietrze_model')
        spark_ml.classificator.Classification.make_class_model(data_pow, sc_pow, powietrze_path, 'RF_pow', 'pm25')

    elif source == "urzedy":
        data_urz, sc_urz = Data_for_ML.urzedy_manipulation.load_urzedy()

        urzedy_path = os.path.join(path, 'urzedy_model')
        spark_ml.reggresor.Regression.make_regr_model(data_urz, sc_urz, urzedy_path, 'RF_urz', "liczbaKlwKolejce")

    elif source == "velib":
        data_vel, sc_vel = Data_for_ML.velib_manipulation.load_velib()

        velib_path = os.path.join(path, 'velib_model')
        spark_ml.reggresor.Regression.make_regr_model(data_vel, sc_vel, velib_path, 'RF_vel', 'numbikesavailable')


def activate_stream(source, spark, sk_connection):
    path = findspark.find()

    if source == "powietrze":
        powietrze_path = os.path.join(path, 'powietrze_model')
        # Uruchomienia modułu szybkiego przetwarzania dla powietrza, który korzysta z wcześniej nauczonych modeli
        query, _ = SpeedLayer.speed_connection.activate_powietrze_stream(model_path=powietrze_path, spark=spark,
                                                                         sk_connection=sk_connection)

    elif source == "urzedy":
        urzedy_path = os.path.join(path, 'urzedy_model')
        query, _ = SpeedLayer.speed_connection.activate_urzedy_stream(model_path=urzedy_path, spark=spark,
                                                                         sk_connection=sk_connection)

    elif source == "velib":
        velib_path = os.path.join(path, 'velib_model')
        query, _ = SpeedLayer.speed_connection.activate_velib_stream(model_path=velib_path, spark=spark,
                                                                         sk_connection=sk_connection)

    return query
