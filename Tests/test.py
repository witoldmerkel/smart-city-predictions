import spark_ml.classificator.Classification
import Data_for_ML.powietrze_manipulation

# Powietrze

data_pow, sc_pow = Data_for_ML.powietrze_manipulation.load_powietrze()

model_pow = spark_ml.classificator.Classification.make_class_model(data_pow, sc_pow)

# Urzedy

data_urz, sc_urz = Data_for_ML.powietrze_manipulation.load_urzedy()

model_urz = spark_ml.classificator.Classification.make_class_model(data_urz, sc_urz)

# Velib

data_vel, sc_vel = Data_for_ML.powietrze_manipulation.load_velib()

model_vel = spark_ml.classificator.Classification.make_class_model(data_vel, sc_vel)
