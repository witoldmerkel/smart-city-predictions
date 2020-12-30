# Importing modules
import pandas as pd
import os
from pyspark.sql import functions as f
from pyspark.sql import SparkSession


# Makes loading faster
def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)


# Spark setup
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.0.0' \
                                    ' --conf spark.cassandra.connection.host=127.0.0.1 pyspark-shell'
spark = SparkSession.builder.getOrCreate()

# Loading data to spark
weather = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table='weather', keyspace='json') \
    .load()

powietrze = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table='powietrze', keyspace='json') \
    .load().withColumn('timezone1', f.when(f.col('name') == 'Paris', "Europe/Paris").otherwise("Europe/Warsaw"))

# Joining data for further analysis
dane_do_agregacji = powietrze.join(weather, powietrze.timezone1 == weather.timezone, how='full')

df1 = dane_do_agregacji.groupBy("timezone").agg(
    f.mean("temp").alias("Mean temperature"),
    f.mean("feels_like").alias("Mean feels like temperature"),
    f.mean("pressure").alias("Mean pressure"),
    f.max("uvi").alias("Maximum uvi"),
    f.min("uvi").alias("Minimum uvi"),
    f.max('visibility').alias("Maximum visibility"),
    f.min('visibility').alias("Minimum visibility"),
    f.skewness("temp").alias("Skewness of temperature"),
    f.mean("pop").alias("Mean chance of rain")
)

df2 = dane_do_agregacji.groupBy("name").agg(
    f.max('co').alias("Maximum CO condensation"),
    f.min('co').alias("Minimum CO condensation"),
    f.max('no2').alias("Maximum NO2 condensation"),
    f.min('no2').alias("Minimum NO2 condensation"),
    f.max('o3').alias("Maximum O3 condensation"),
    f.min('o3').alias("Minimum O3 condensation"),
    f.max('pm10').alias("Maximum PM10 condensation"),
    f.min('pm10').alias("Minimum PM10 condensation"),
    f.max('pm25').alias("Maximum PM2.5 condensation"),
    f.min('pm25').alias("Minimum PM2.5 condensation"),
    f.mean('pm25').alias("Mean PM2.5 condensation")
)

df3 = dane_do_agregacji.groupBy("weather_main").agg(
    f.count("weather_main").alias("Number of weather type")
)

df4 = dane_do_agregacji.groupBy("weather_description").agg(
    f.count("weather_description").alias("Number of different descriptions")
)

df1.show()
df2.show()
df3.show()
df4.show()
