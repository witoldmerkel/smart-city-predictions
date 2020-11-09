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

powietrze = powietrze.withColumn("target", (when(powietrze["pm25"] < 12, "Dobre").when(powietrze["pm25"] <= 35, "Umiarkowane")
                                            .when(powietrze["pm25"] <= 55, "Niezdrowe dla chorych").when(powietrze["pm25"] <= 150, "Niezdrowe")
                                            .when(powietrze["pm25"] <= 250, "Bardzo niezdrowe").otherwise("Niebezpieczne")))

iterator1 = powietrze.select('name').distinct().orderBy(powietrze['name'])
iterator = iterator1.toPandas()
num = 0

w = Window().partitionBy().orderBy(col("timestamp"))
for id in iterator['name']:
    temp = powietrze.where(powietrze['name'] == id)
    temp = temp.orderBy(temp["timestamp"])
    temp = temp.select("*", lag("target", 24).over(w).alias("target(stan_za_4h)")).na.drop()
    if num == 0:
        dane = temp
    else:
        dane = dane.union(temp)
    num+=1

dane.show()