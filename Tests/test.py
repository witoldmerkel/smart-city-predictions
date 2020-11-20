import spark_ml.classificator.Classification
import spark_ml.reggresor.Regression
import Data_for_ML.powietrze_manipulation
import Data_for_ML.urzedy_manipulation
import Data_for_ML.velib_manipulation
import SpeedLayer.speed_connection

# Powietrze

data_pow, sc_pow = Data_for_ML.powietrze_manipulation.load_powietrze()

model_pow = spark_ml.classificator.Classification.make_class_model(data_pow, sc_pow, r'D:\powietrze_model')

spark_pow, query_pow, ssc_pow = SpeedLayer.speed_connection.activate_powietrze_stream()

# Urzedy

data_urz, sc_urz = Data_for_ML.urzedy_manipulation.load_urzedy()

model_urz = spark_ml.reggresor.Regression.make_regr_model(data_urz, sc_urz, r'D:\urzedy_model')

spark_urz, query_urz, ssc_urz = SpeedLayer.speed_connection.activate_urzedy_stream()

# Velib

data_vel, sc_vel = Data_for_ML.velib_manipulation.load_velib()

model_vel = spark_ml.reggresor.Regression.make_regr_model(data_vel, sc_vel, r'D:\velib_model')

spark_vel, query_vel, ssc_vel = SpeedLayer.speed_connection.activate_powietrze_stream()