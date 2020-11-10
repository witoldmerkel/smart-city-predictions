import os
import platform
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import hour, minute, year, month, dayofweek, when, lag, col
from pyspark.sql.window import Window

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.0.0 --conf spark.cassandra.connection.host=127.0.0.1 pyspark-shell'

sc = SparkContext("local", "json")
sqlContext = SQLContext(sc)

def load_and_get_table_df(keys_space_name, table_name):
    table_df = sqlContext.read\
        .format("org.apache.spark.sql.cassandra")\
        .options(table=table_name, keyspace=keys_space_name)\
        .load()
    return table_df

# Wczytanie danych

velib1 = load_and_get_table_df("json", "velib")

# Dodanie zmiennych opisujących dokładnie czas

velib1 = velib1.withColumn("normal_type", velib1["timestamp"].cast(TimestampType()))
godzina = velib1.withColumn('godzina', hour(velib1['normal_type']))
minuta = godzina.withColumn('minuta', minute(godzina['normal_type']))
rok = minuta.withColumn('rok', year(minuta['normal_type']))
miesiac = rok.withColumn('miesiac', month(rok['normal_type']))
velib = miesiac.withColumn("dzien", dayofweek(miesiac["normal_type"]))

# Stworzenie targeta

iterator1 = velib.select('station_id').distinct().orderBy(velib['station_id'])
iterator = iterator1.toPandas()
num = 0

w = Window().partitionBy().orderBy(col("timestamp"))
for id in iterator['station_id']:
    temp = velib.where(velib['station_id'] == id)
    temp = temp.orderBy(temp["timestamp"])
    temp = temp.select("*", lag("num_bikes_available", 240).over(w).alias("num_bikes_available(stan_za_4h)")).na.drop()
    if num == 0:
        dane = temp
    else:
        dane = dane.union(temp)
    num+=1

dane.show()

# Zamkniecie polaczenia ze Spark

sc.stop()

plt = platform.system()

if plt == "Windows":
    os.system('rmdir /q /s "D:\SparkTEMP"')