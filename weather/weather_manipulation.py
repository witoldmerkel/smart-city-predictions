# Importing modules
import pandas as pd
import os
from pyspark.sql import functions as f
from pyspark.sql import SparkSession

# Makes loading faster

def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)

# Spark setup
os.environ[
        'PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.0.0' \
                                 ' --conf spark.cassandra.connection.host=127.0.0.1 pyspark-shell'
spark = SparkSession.builder.getOrCreate()


# Loading data to spark
weather = spark.read\
        .format("org.apache.spark.sql.cassandra")\
        .options(table='weather', keyspace='json')\
        .load()

powietrze = spark.read\
        .format("org.apache.spark.sql.cassandra")\
        .options(table='powietrze', keyspace='json')\
        .load()

weather.show()
powietrze.show()
