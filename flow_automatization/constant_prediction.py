from functions_wrapers import activate_stream
from shared_connection import prepare_sk_connection
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from speed_layer import spark_cassandra_connection
import time
import os

# Funkcja automatyzująca ciągłe wykonywanie predykcji
# Przed modyfikacją źródeł wyczyścić folder "checkpoints" w lokalizacji Spark!


def start_flow_predictions(list_of_sources=["velib", "powietrze", "urzedy"], refresh_time=86400):

    while True:
        # Wymagane pakiety
        os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,' \
                                            'org.apache.spark:spark-streaming-kafka-0-10-assembly_2.12:3.0.1,' \
                                            'org.apache.kafka:kafka-clients:2.6.0,' \
                                            'org.apache.commons:commons-pool2:2.9.0,' \
                                            'com.datastax.spark:spark-cassandra-connector_2.12:3.0.0,' \
                                            'org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.0.1,' \
                                            ' --conf spark.cassandra.connection.host=127.0.0.1 pyspark-shell'

        # Stworzenie sesji Spark zarządzającej strumieniami
        spark = SparkSession \
            .builder \
            .appName("StreamingApp") \
            .getOrCreate()

        # Stworzenie wspólnego połączenia czytającego z wielu topiców Kafka
        sk_connection, spark, topics = prepare_sk_connection(list_of_sources, spark)
        streams = []
        # Filtrowanie danych ze względu na topic
        for topic in topics:
            streams.append(sk_connection.filter(F.col("topic") == topic))

        connections = []
        # Tworzenie strumieni
        for i, source in enumerate(list_of_sources):
            query = activate_stream(source, spark, streams[i])
            time.sleep(10)
            connections.append(query)

        # Laczenie strumieni
        result_stream = connections[0]
        for i, connection in enumerate(connections):
            if i == 0:
                continue
            else:
                result_stream = result_stream.union(connections[i])

        # Aktywacja strumieni i połączenia z bazą danych Cassandra
        query = spark_cassandra_connection.writeToCassandra(result_stream)
        print("starting...")
        spark.streams.awaitAnyTermination(refresh_time)
        time.sleep(10)
        # Zamykanie otwartych połączeń ze Spark
        query.stop()
        spark.stop()


if __name__ == "__main__":
    start_flow_predictions(list_of_sources=["velib", "powietrze", "urzedy"])
