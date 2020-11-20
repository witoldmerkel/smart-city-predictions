import os
from pyspark.sql import SparkSession


def writeToCassandra(stream, keyspace, table):

    query = stream.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .outputMode('append') \
        .options(table=table, keyspace=keyspace, checkpointLocation=r'D:\checkpoint') \
        .start() \
        .awaitTermination()

    return query

