# Importing modules
import os
from pyspark.sql import functions as f
from pyspark.sql import SparkSession

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

# Aggregating data
df1 = dane_do_agregacji.groupBy("timezone").agg(
    f.mean("temp").alias("mean_temperature"),
    f.mean("feels_like").alias("mean_feels_like_temperature"),
    f.mean("pressure").alias("mean_pressure"),
    f.max("uvi").alias("maximum_uvi"),
    f.min("uvi").alias("minimum_uvi"),
    f.max('visibility').alias("maximum_visibility"),
    f.min('visibility').alias("minimum_visibility"),
    f.skewness("temp").alias("skewness_of_temperature"),
    f.mean("pop").alias("mean_chance_of_rain")
)

df2 = dane_do_agregacji.groupBy("name").agg(
    f.max('co').alias("maximum_co_condensation"),
    f.min('co').alias("minimum_co_condensation"),
    f.max('no2').alias("maximum_no2_condensation"),
    f.min('no2').alias("minimum_no2_condensation"),
    f.max('o3').alias("maximum_o3_condensation"),
    f.min('o3').alias("minimum_o3_condensation"),
    f.max('pm10').alias("maximum_pm10_condensation"),
    f.min('pm10').alias("minimum_pm10_condensation"),
    f.max('pm25').alias("maximum_pm25_condensation"),
    f.min('pm25').alias("minimum_pm25_condensation"),
    f.mean('pm25').alias("mean_pm25_condensation")
)

df3 = dane_do_agregacji.groupBy("weather_main", "timezone").agg(
    f.count("weather_main").alias("number_of_weather_type")
)

df4 = dane_do_agregacji.groupBy("weather_description", "timezone").agg(
    f.count("weather_description").alias("number_of_different_descriptions")
)

# Saving data to Apache Cassandra
df1\
    .write\
    .format("org.apache.spark.sql.cassandra")\
    .mode("append")\
    .options(table="aggregates1", keyspace="weather_aggregations")\
    .save()

df2\
    .write\
    .format("org.apache.spark.sql.cassandra")\
    .mode("append")\
    .options(table="aggregates2", keyspace="weather_aggregations")\
    .save()

df3\
    .write\
    .format("org.apache.spark.sql.cassandra")\
    .mode("append")\
    .options(table="aggregates3", keyspace="weather_aggregations")\
    .save()

df4\
    .write\
    .format("org.apache.spark.sql.cassandra")\
    .mode("append")\
    .options(table="aggregates4", keyspace="weather_aggregations")\
    .save()
