import os
from pyspark.sql import SparkSession


def writeToCassandra(stream, keyspace, table):
    os.environ[
        'PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.0.0' \
                                 ' --conf spark.cassandra.connection.host=127.0.0.1 pyspark-shell'

    def forEachCassandra(writeDF, epochId):

        writeDF.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode('append') \
            .options(table=table, keyspace=keyspace) \
            .save()

    query = stream.writeStream \
        .foreachBatch(forEachCassandra) \
        .outputMode("update") \
        .start()

    query.awaitTermination()

    return query

