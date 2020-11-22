import spark_ml.classificator.Classification
import spark_ml.reggresor.Regression
import Data_for_ML.powietrze_manipulation
import Data_for_ML.urzedy_manipulation
import Data_for_ML.velib_manipulation
import SpeedLayer.speed_connection
import os
import findspark

path = findspark.find()

# Miejse do testowania działania modułów

# Powietrze

# Załadowanie tabeli powietrze z bazy danych master dataset
data_pow, sc_pow = Data_for_ML.powietrze_manipulation.load_powietrze()

# Wytrenowanie modelu klsyfikacyjnego na wcześniej załadowanych danych
powietrze_path = os.path.join(path, 'powietrze_model')
model_pow = spark_ml.classificator.Classification.make_class_model(data_pow, sc_pow, powietrze_path, 'RF_pow',
                                                                   'pm25')

# Uruchomienia modułu szybkiego przetwarzania dla powietrza, który korzysta z wcześniej nauczonych modeli
spark_pow, query_pow, ssc_pow = SpeedLayer.speed_connection.activate_powietrze_stream(model_path=powietrze_path)

# Urzedy

data_urz, sc_urz = Data_for_ML.urzedy_manipulation.load_urzedy()

urzedy_path = os.path.join(path, 'urzedy_model')
model_urz = spark_ml.reggresor.Regression.make_regr_model(data_urz, sc_urz, urzedy_path, 'RF_urz',
                                                          "liczbaKlwKolejce")

spark_urz, query_urz, ssc_urz = SpeedLayer.speed_connection.activate_urzedy_stream(model_path=urzedy_path)

# Velib

data_vel, sc_vel = Data_for_ML.velib_manipulation.load_velib()

velib_path = os.path.join(path, 'velib_model')
model_vel = spark_ml.reggresor.Regression.make_regr_model(data_vel, sc_vel, velib_path, 'RF_vel',
                                                          'numbikesavailable')

spark_vel, query_vel, ssc_vel = SpeedLayer.speed_connection.activate_velib_stream(model_path=velib_path)
