import os
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

powietrze1 = load_and_get_table_df("json", "powietrze")

# Dodanie zmiennych opisujących dokładnie czas

powietrze1 = powietrze1.withColumn("normal_type", powietrze1["timestamp"].cast(TimestampType()))
godzina = powietrze1.withColumn('godzina', hour(powietrze1['normal_type']))
minuta = godzina.withColumn('minuta', minute(godzina['normal_type']))
rok = minuta.withColumn('rok', year(minuta['normal_type']))
miesiac = rok.withColumn('miesiac', month(rok['normal_type']))
powietrze = miesiac.withColumn("dzien", dayofweek(miesiac["normal_type"]))

# Stworzenie targeta

powietrze = powietrze.withColumn("target(stan_za_4h)", (when(powietrze["pm25"] < 12, "Dobre").when(powietrze["pm25"] <= 35, "Umiarkowane")
                                            .when(powietrze["pm25"] <= 55, "Niezdrowe dla chorych").when(powietrze["pm25"] <= 150, "Niezdrowe")
                                            .when(powietrze["pm25"] <= 250, "Bardzo niezdrowe").otherwise("Niebezpieczne")))

paryz = powietrze.where(powietrze["name"] == "Paris")
ursynow = powietrze.where(powietrze["name"] == "Ursynów, Warszawa, Mazowieckie, Poland")
marszalkowska = powietrze.where(powietrze["name"] == "Marszałkowska, Warszawa, Mazowieckie, Poland")
targowek = powietrze.where(powietrze["name"] == "Targówek, Warszawa, Mazowieckie, Poland")

paryz = paryz.orderBy(paryz["timestamp"])
ursynow = ursynow.orderBy(ursynow["timestamp"])
marszalkowska = marszalkowska.orderBy(marszalkowska["timestamp"])
targowek = targowek.orderBy(targowek["timestamp"])

w = Window().partitionBy().orderBy(col("timestamp"))
paryz = paryz.select("*", lag("target(stan_za_4h)", 24).over(w).alias("target")).na.drop()
ursynow = ursynow.select("*", lag("target(stan_za_4h)", 24).over(w).alias("target")).na.drop()
marszalkowska = marszalkowska.select("*", lag("target(stan_za_4h)", 24).over(w).alias("target")).na.drop()
targowek = targowek.select("*", lag("target(stan_za_4h)", 24).over(w).alias("target")).na.drop()

paryz = paryz.drop("target")
ursynow = ursynow.drop("target")
marszalkowska = marszalkowska.drop("target")
targowek = targowek.drop("target")