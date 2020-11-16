from pyspark.sql.functions import from_json
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.types import (
        StructType, StringType, IntegerType
        )
import os


def activate_speed_connection(topic="sparkvelib", model_path=r'C:\Users\jaiko\Desktop\Inżynierka\class_model'):


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

    # loaded_model = PipelineModel.load(model_path)

    # Consume Kafka topic
    sc = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092")\
        .option("subscribe", topic).option("startingOffsets","earliest").load()

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

    stream.select("parsed.*")

    query = stream.writeStream.outputMode('append').option("truncate", False).format('console').start()

    query.awaitTermination()

activate_speed_connection()