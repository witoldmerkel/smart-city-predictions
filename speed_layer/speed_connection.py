from pyspark.sql.functions import from_json
import pyspark.sql.functions as F
from kafka_spark_connection import create_sk_connection
from velib_manipulation import velib_preprocessing
from powietrze_manipulation import powietrze_preprocessing
from urzedy_manipulation import urzedy_preprocessing
from spark_cassandra_connection import writeToCassandra
from streams_handling import stream_to_predictions
from pyspark.sql.types import (
        StructType, StringType, IntegerType, FloatType
        )
# W tym pliku znajdują się funkcje, które zajmują się tworzeniem i obsługą strumieni danych dla każdego źródła danych


def activate_velib_stream(topic="sparkvelib", model_path=r'velib_model', target="numbikesavailable",
                          source_name="velib", spark=None, sk_connection=None, put_cassandra=False,
                          agg=None, time_frames="5 minutes", time_update="1 minute"):
    # Stworzenie połączenia kafka-spark dla wybranego tematu
    if sk_connection is None:
        sc, spark = create_sk_connection(topic, spark)
    else:
        sc = sk_connection
    # Definicja struktury pliku json, który zostanie pobrany
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
    # Parsowanie otrzymanego pliku
    stream = sc.select(
       from_json(F.col("value").cast("string"), json_schema).alias("parsed")
    )

    stream = stream.select("parsed.*")
    # Uzgadnianie typów
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
    # Wstępne przetwarzanie danych wspólne dla przygotowania danych do uczenia modelu oraz dla wykonywania predykcji
    stream = velib_preprocessing(stream, agg, time_frames, time_update)
    # Przekształcanie strumienia danych do predykcji
    stream = stream_to_predictions(stream, model_path, target, source_name)
    # One table
    stream = stream.withColumn('individual', stream['station_id'])

    stream = stream.select('prediction', 'individual', "source_name", "timestamp", "target_column", "model_path")
    query = stream
    # Zapisywanie strumienia do tablicy w bazie danych Cassandra
    if put_cassandra:
        query = writeToCassandra(stream=stream)

    return query, spark


def activate_powietrze_stream(topic="sparkpowietrze", model_path=r'powietrze_model', target="pm25",
                              source_name="powietrze", spark=None, sk_connection=None, put_cassandra=False,
                              agg=None, time_frames="5 minutes", time_update="1 minute"):
    # Stworzenie połączenia kafka-spark dla wybranego tematu
    if sk_connection is None:
        sc, spark = create_sk_connection(topic, spark)
    else:
        sc = sk_connection
    # Definicja struktury pliku json, który zostanie pobrany
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
    # Parsowanie otrzymanego pliku
    stream = sc.select(
       from_json(F.col("value").cast(StringType()), json_schema).alias("parsed")
    )

    stream = stream.select("parsed.*")
    # Uzgadnianie typów
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
    # Wstępne przetwarzanie danych wspólne dla przygotowania danych do uczenia modelu oraz dla wykonywania predykcji
    stream = powietrze_preprocessing(stream, agg, time_frames, time_update)
    stream = stream_to_predictions(stream, model_path, target, source_name)
    # One table
    stream = stream.withColumn('individual', stream['name'])

    stream = stream.select('prediction', 'individual', "source_name", "timestamp", "target_column", "model_path")
    # Zapisywanie strumienia do tablicy w bazie danych Cassandra
    query = stream
    # Zapisywanie strumienia do tablicy w bazie danych Cassandra
    if put_cassandra:
        query = writeToCassandra(stream=stream)

    return query, spark


def activate_urzedy_stream(topic="sparkurzedy", model_path=r'urzedy_model', target="liczbaKlwKolejce",
                           source_name="urzedy", spark=None, sk_connection=None, put_cassandra=False,
                           agg=None, time_frames="5 minutes", time_update="1 minute"):
    # Stworzenie połączenia kafka-spark dla wybranego tematu
    if sk_connection is None:
        sc, spark = create_sk_connection(topic, spark)
    else:
        sc = sk_connection
    # Definicja struktury pliku json, który zostanie pobrany
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
    # Parsowanie otrzymanego pliku
    stream = sc.select(
       from_json(F.col("value").cast("string"), json_schema).alias("parsed")
    )

    stream = stream.select("parsed.*")
    # Uzgadnianie typów
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
    # Wstępne przetwarzanie danych wspólne dla przygotowania danych do uczenia modelu oraz dla wykonywania predykcji
    stream = urzedy_preprocessing(stream, agg, time_frames, time_update)
    stream = stream_to_predictions(stream, model_path, target, source_name)
    # One table
    stream = stream.withColumn('individual', stream['idgrupy'])

    stream = stream.select('prediction', 'individual', "source_name", "timestamp", "target_column", "model_path")
    # Zapisywanie strumienia do tablicy w bazie danych Cassandra
    query = stream

    # Zapisywanie strumienia do tablicy w bazie danych Cassandra
    if put_cassandra:
        query = writeToCassandra(stream=stream)

    return query, spark
