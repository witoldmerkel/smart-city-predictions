from pyspark.sql.functions import from_json
import pyspark.sql.functions as F
from kafka_spark_connection import create_sk_connection
from velib_manipulation import velib_preprocessing
from powietrze_manipulation import powietrze_preprocessing
from urzedy_manipulation import urzedy_preprocessing
from spark_cassandra_connection import writeToCassandra
from streams_handling import stream_to_predictions
from pyspark.ml import PipelineModel
from pyspark.sql.types import (
        StructType, StringType, IntegerType, FloatType
        )



def activate_velib_stream(topic="sparkvelib", model_path=r'C:\Users\jaiko\Desktop\Inżynierka\class_model', table = ""):

    sc = create_sk_connection(topic)

    json_schema = StructType() \
        .add("station_code", StringType()) \
        .add("station_id", StringType(), False) \
        .add("num_bikes_available", StringType()) \
        .add("numbikesavailable", StringType()) \
        .add("mechanical", StringType()) \
        .add("ebike", StringType()) \
        .add("num_docks_available", StringType()) \
        .add("numdocksavailable", StringType()) \
        .add("is_installed", StringType()) \
        .add("is_returning", StringType()) \
        .add("is_renting", StringType()) \
        .add("timestamp", StringType(), False)

    stream = sc.select(
       from_json(F.col("value").cast("string"), json_schema).alias("parsed")
    )

    stream = stream.select("parsed.*")

    stream = stream.withColumn("is_installed", stream['is_installed'].cast(StringType())) \
        .withColumn('is_renting', stream['is_renting'].cast(StringType())) \
        .withColumn('is_returning', stream['is_returning'].cast(StringType())) \
        .withColumn('ebike', stream['ebike'].cast(IntegerType())) \
        .withColumn('mechanical', stream['mechanical'].cast(IntegerType())) \
        .withColumn('num_bikes_available', stream['num_bikes_available'].cast(IntegerType())) \
        .withColumn('num_docks_available', stream['num_docks_available'].cast(IntegerType())) \
        .withColumn('numbikesavailable', stream['numbikesavailable'].cast(IntegerType())) \
        .withColumn('numdocksavailable', stream['numdocksavailable'].cast(IntegerType())) \
        .withColumn('station_code', stream['station_code'].cast(IntegerType())) \
        .withColumn('station_id', stream['station_id'].cast(IntegerType())) \
        .withColumn('timestamp', stream['timestamp'].cast(IntegerType()))

    stream = velib_preprocessing(stream)

    query = stream.writeStream.outputMode('append').option("truncate", False).format('console').start()

    query.awaitTermination()

    return stream, query, model_path, table


def activate_powietrze_stream(topic="sparkpowietrze", model_path=r'C:\Users\jaiko\Desktop\Inżynierka\class_model',
                              keyspace = "predictions", table = "powietrze_predictions", target = "pm25"):

    sc = create_sk_connection(topic)

    json_schema = StructType() \
        .add("o3", StringType()) \
        .add("tz", StringType()) \
        .add("h", StringType()) \
        .add("pm10", StringType()) \
        .add("co", StringType()) \
        .add("long", StringType()) \
        .add("no2", StringType()) \
        .add("p", StringType()) \
        .add("pm25", StringType()) \
        .add("t", StringType()) \
        .add("v", StringType()) \
        .add("so2", StringType()) \
        .add("w", StringType()) \
        .add("name", StringType()) \
        .add("lat", StringType()) \

    stream = sc.select(
       from_json(F.col("value").cast(StringType()), json_schema).alias("parsed")
    )

    stream = stream.select("parsed.*")

    stream = stream.withColumn("co", stream['co'].cast(FloatType())) \
        .withColumn('h', stream['h'].cast(FloatType())) \
        .withColumn('lat', stream['lat'].cast(FloatType())) \
        .withColumn('long', stream['long'].cast(FloatType())) \
        .withColumn('name', stream['name'].cast(StringType())) \
        .withColumn('no2', stream['no2'].cast(FloatType())) \
        .withColumn('o3', stream['o3'].cast(FloatType())) \
        .withColumn('p', stream['p'].cast(FloatType())) \
        .withColumn('pm10', stream['pm10'].cast(FloatType())) \
        .withColumn('pm25', stream['pm25'].cast(FloatType())) \
        .withColumn('so2', stream['so2'].cast(FloatType())) \
        .withColumn('t', stream['t'].cast(FloatType())) \
        .withColumn('tz', stream['tz'].cast(StringType())) \
        .withColumn('w', stream['w'].cast(FloatType())) \
        .withColumn('timestamp', stream['v'].cast(IntegerType()))

    stream = powietrze_preprocessing(stream)

    stream = stream_to_predictions(stream, model_path, target)

    stream = stream.select('predictedlabel', "name", "timestamp", "target_column", "model_path")

    query = writeToCassandra(stream=stream, keyspace=keyspace, table=table)


    return stream, query, sc


def activate_urzedy_stream(topic="sparkurzedy", model_path=r'C:\Users\jaiko\Desktop\Inżynierka\class_model',
                           table = ""):

    sc = create_sk_connection(topic)

    json_schema = StructType() \
        .add("timestamp", StringType()) \
        .add("lp", StringType()) \
        .add("czasObslugi", StringType()) \
        .add("liczbaCzynnychStan", StringType()) \
        .add("nazwaGrupy", StringType()) \
        .add("literaGrupy", StringType()) \
        .add("liczbaKlwKolejce", StringType()) \
        .add("idGrupy", StringType()) \
        .add("aktualnyNumer", StringType()) \
        .add("status", StringType()) \

    stream = sc.select(
       from_json(F.col("value").cast("string"), json_schema).alias("parsed")
    )

    stream = stream.select("parsed.*")

    stream = stream.withColumn("timestamp", stream['timestamp'].cast(IntegerType())) \
        .withColumn('l_p', stream['lp'].cast(StringType())) \
        .withColumn('czasobslugi', stream['czasObslugi'].cast(IntegerType())) \
        .withColumn('liczbaczynnychstan', stream['liczbaCzynnychStan'].cast(IntegerType())) \
        .withColumn('nazwagrupy', stream['nazwaGrupy'].cast(StringType())) \
        .withColumn('literagrupy', stream['literaGrupy'].cast(StringType())) \
        .withColumn('liczbaklwkolejce', stream['liczbaKlwKolejce'].cast(IntegerType())) \
        .withColumn('idgrupy', stream['idGrupy'].cast(IntegerType())) \
        .withColumn('aktualny_numer', stream['aktualnyNumer'].cast(StringType())) \
        .withColumn('status', stream['status'].cast(StringType()))

    stream = urzedy_preprocessing(stream)

    query = stream.writeStream.outputMode('append').option("truncate", False).format('console').start()

    query.awaitTermination()

    return stream, query, model_path, table


activate_powietrze_stream()
