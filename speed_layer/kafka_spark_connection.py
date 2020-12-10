from pyspark.sql import SparkSession
import os

# W tym pliku znajdują się funkcję odpowiedzialne za tworzenia i obsługiwanie polączeń kafka-spark


def create_sk_connection(topic, spark=None):
# Wymagane pakiety
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,' \
                                    'org.apache.spark:spark-streaming-kafka-0-10-assembly_2.12:3.0.1,' \
                                    'org.apache.kafka:kafka-clients:2.6.0,' \
                                    'org.apache.commons:commons-pool2:2.9.0,' \
                                    'com.datastax.spark:spark-cassandra-connector_2.12:3.0.0,' \
                                    'org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.0.1,' \
                                    ' --conf spark.cassandra.connection.host=127.0.0.1 pyspark-shell'
    if spark is None:
    # Tworzenie sesji spark
        spark = SparkSession\
                .builder\
                .appName(topic)\
                .getOrCreate()

    # Stworzenie strumienia pobierania danych z tematu Kafka
    sc = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092")\
        .option("subscribe", topic).option("startingOffsets", "earliest").option("encoding", "UTF-8")\
        .option("failOnDataLoss", "false").load()

    return sc, spark
