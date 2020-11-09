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

urzedy1 = load_and_get_table_df("json", "urzedy")
urzedy1 = urzedy1.drop(*['czasobslugi', 'l_p', 'status', 'aktualny_numer'])

# Dodanie zmiennych opisujących dokładnie czas

urzedy1 = urzedy1.withColumn("normal_type", urzedy1["timestamp"].cast(TimestampType()))
godzina = urzedy1.withColumn('godzina', hour(urzedy1['normal_type']))
minuta = godzina.withColumn('minuta', minute(godzina['normal_type']))
rok = minuta.withColumn('rok', year(minuta['normal_type']))
miesiac = rok.withColumn('miesiac', month(rok['normal_type']))
urzedy = miesiac.withColumn("dzien", dayofweek(miesiac["normal_type"]))

# Stworzenie targeta

iterator1 = urzedy.select('idgrupy').distinct().orderBy(urzedy['idgrupy'])
iterator = iterator1.toPandas()
num = 0

w = Window().partitionBy().orderBy(col("timestamp"))
for id in iterator['idgrupy']:
    temp = urzedy.where(urzedy['idgrupy'] == id)
    temp = temp.orderBy(temp["timestamp"])
    temp = temp.select("*", lag("liczbaklwkolejce", 240).over(w).alias("liczbaklwkolejce(stan_za_4h)")).na.drop()
    if num == 0:
        dane = temp
    else:
        dane = dane.union(temp)
    num+=1

dane.show()