from pyspark.sql import SparkSession
import os


def create_sk_connection(topic):
# All of these jars should be downloaded in spark jars
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,' \
                                        'org.apache.spark:spark-streaming-kafka-0-10-assembly_2.12:3.0.1,' \
                                        'org.apache.kafka:kafka-clients:2.6.0,' \
                                        'org.apache.commons:commons-pool2:2.9.0,' \
                                        'org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.0.1 pyspark-shell'

    # Creating spark session
    spark = SparkSession\
            .builder\
            .appName(topic)\
            .getOrCreate()

    # Consume Kafka topic
    sc = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092")\
        .option("subscribe", topic).option("startingOffsets", "earliest").load()

    return sc
