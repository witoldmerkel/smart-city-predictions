from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
import os

#All of these jars should be downloaded in spark jars
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,' \
                                    'org.apache.spark:spark-streaming-kafka-0-10-assembly_2.12:3.0.1,' \
                                    'org.apache.kafka:kafka-clients:2.6.0,' \
                                    'org.apache.commons:commons-pool2:2.9.0,' \
                                    'org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.0.1 pyspark-shell'

spark = SparkSession\
        .builder\
        .appName("StructuredKafkaWordCount")\
        .getOrCreate()

# Consume Kafka topic
events = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe", "sparkvelib").load()

# Cast the JSON payload as a String
lines = events.selectExpr("CAST(value AS STRING)")

words = lines.select(
    # explode turns each item in an array into a separate row
    explode(
        split(lines.value, ' ')
    ).alias('word')
)

# Generate running word count
wordCounts = words.groupBy('word').count()
# Start running the query that prints the running counts to the console
query = wordCounts\
    .writeStream\
    .outputMode('complete')\
    .format('console')\
    .start()

query.awaitTermination()