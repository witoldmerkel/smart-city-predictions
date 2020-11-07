import os
from pyspark import SparkContext
from pyspark.sql import SQLContext
import platform


os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.0.0 --conf spark.cassandra.connection.host=127.0.0.1 pyspark-shell'

sc = SparkContext("local", "json")
sqlContext = SQLContext(sc)


def load_and_get_table_df(keys_space_name, table_name):
    table_df = sqlContext.read\
        .format("org.apache.spark.sql.cassandra")\
        .options(table=table_name, keyspace=keys_space_name)\
        .load()
    return table_df


velib = load_and_get_table_df("json", "velib")


velib.show()
velib.groupBy("station_id").count().orderBy('count', ascending=False).show()

velib.printSchema()

firstStation = velib.where(velib["station_id"] == 102328355).select("ebike")

# Teraz dopiero zachodzi pobranie

firstStation = firstStation.cache()

# Usuwanie danych z pamięci

firstStation.unpersist()

# Shutdowning PySpark Context


sc.stop()

plt = platform.system()

if plt == "Windows":
    os.system('rmdir /q /s "D:\SparkTEMP"')