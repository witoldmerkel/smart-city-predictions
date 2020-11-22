import os
import findspark
# W tym pliku znajdują się funkcje odpowiedzialne za tworzenie i obsługę połączeń na lini spark-cassadnra

def writeToCassandra(stream):
    # Tworzenia połączenia spark-cassandra oraz zapisywanie strumienia danych do bazy danych Cassandra

    checkpointLocation = os.path.join(findspark.find(), "checkpoint")

    query = stream.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .outputMode('append') \
        .options(table="predictions", keyspace="predictions", checkpointLocation=checkpointLocation) \
        .start() \
        .awaitTermination()

    return query

