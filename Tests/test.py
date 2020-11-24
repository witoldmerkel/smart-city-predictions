import spark_ml.classificator.Classification
import spark_ml.reggresor.Regression
import Data_for_ML.powietrze_manipulation
import Data_for_ML.urzedy_manipulation
import Data_for_ML.velib_manipulation
import SpeedLayer.speed_connection
import os
import findspark
from pyspark.sql import SparkSession
from shared_connection import prepare_sk_connection
from pyspark.sql import functions as F

path = findspark.find()

# Miejse do testowania działania modułów

# Powietrze

# Załadowanie tabeli powietrze z bazy danych master dataset
data_pow, sc_pow = Data_for_ML.powietrze_manipulation.load_powietrze()

# Wytrenowanie modelu klsyfikacyjnego na wcześniej załadowanych danych
powietrze_path = os.path.join(path, 'powietrze_model')
spark_ml.classificator.Classification.make_class_model(data_pow, sc_pow, powietrze_path, 'RF_pow', 'pm25')

# Uruchomienia modułu szybkiego przetwarzania dla powietrza, który korzysta z wcześniej nauczonych modeli
query_pow, ssc_pow = SpeedLayer.speed_connection.activate_powietrze_stream(model_path=powietrze_path, put_cassandra=True)

# Urzedy

data_urz, sc_urz = Data_for_ML.urzedy_manipulation.load_urzedy()

urzedy_path = os.path.join(path, 'urzedy_model')
spark_ml.reggresor.Regression.make_regr_model(data_urz, sc_urz, urzedy_path, 'RF_urz', "liczbaKlwKolejce")

query_urz, ssc_urz = SpeedLayer.speed_connection.activate_urzedy_stream(model_path=urzedy_path, put_cassandra=True)

# Velib

data_vel, sc_vel = Data_for_ML.velib_manipulation.load_velib()

velib_path = os.path.join(path, 'velib_model')
spark_ml.reggresor.Regression.make_regr_model(data_vel, sc_vel, velib_path, 'RF_vel', 'numbikesavailable')

query_vel, ssc_vel = SpeedLayer.speed_connection.activate_velib_stream(model_path=velib_path, put_cassandra=True)


# Shared conncection

spark = SparkSession \
            .builder \
            .appName("StreamingApp") \
            .getOrCreate()

list_of_sources = ["powietrze", "urzedy"]

sk_connection, spark, topics = prepare_sk_connection(list_of_sources, spark)

streams = []
for topic in topics:
    streams.append(sk_connection.filter(F.col("topic") == topic))

query1 = streams[0] \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query2 = streams[1] \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()


spark.streams.awaitAnyTermination()
