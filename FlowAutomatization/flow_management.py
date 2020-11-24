from functions_wrapers import load_and_train, activate_stream
from shared_connection import prepare_sk_connection
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from SpeedLayer import spark_cassandra_connection
import time
import os

# Funkcja automatyzująca cykliczne wykonywania pobierania danych i uczenia modeli oraz ciągłego wykonywania predykcji


def start_flow(list_of_sources=["velib", "powietrze", "urzedy"], refresh_time=86400):

    # Inicjalizacja pierwszych modeli
    # for source in list_of_sources:
    #     load_and_train(source)
    #     time.sleep(10)

    while True:
        # Wymagane pakiety
        os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,' \
                                            'org.apache.spark:spark-streaming-kafka-0-10-assembly_2.12:3.0.1,' \
                                            'org.apache.kafka:kafka-clients:2.6.0,' \
                                            'org.apache.commons:commons-pool2:2.9.0,' \
                                            'com.datastax.spark:spark-cassandra-connector_2.12:3.0.0,' \
                                            'org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.0.1,' \
                                            ' --conf spark.cassandra.connection.host=127.0.0.1 pyspark-shell'
        # sessions = []
        spark = SparkSession \
            .builder \
            .appName("StreamingApp") \
            .getOrCreate()

        # sessions.append(spark)
        #
        # for _ in list_of_sources[1:]:
        #     sessions.append(spark.newSession())
        sk_connection, spark, topics = prepare_sk_connection(list_of_sources, spark)
        streams = []
        for topic in topics:
            streams.append(sk_connection.filter(F.col("topic") == topic))

        connections = []
        for i, source in enumerate(list_of_sources):
            query = activate_stream(source, spark, streams[i])
            time.sleep(10)
            connections.append(query)

        result_stream = connections[0]
        for i, connection in enumerate(connections):
            if i == 0:
                continue
            else:
                result_stream = result_stream.union(connections[i])

        query = spark_cassandra_connection.writeToCassandra(result_stream)
        spark.streams.awaitAnyTermination()
        time.sleep(refresh_time)

        query.stop()
        spark.stop()

        for source in list_of_sources:
            load_and_train(source)
            time.sleep(10)


start_flow(list_of_sources=["powietrze", "urzedy"])
