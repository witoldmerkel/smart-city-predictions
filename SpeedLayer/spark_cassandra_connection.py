# W tym pliku znajdują się funkcje odpowiedzialne za tworzenie i obsługę połączeń na lini spark-cassadnra

def writeToCassandra(stream):
    # Tworzenia połączenia spark-cassandra oraz zapisywanie strumienia danych do bazy danych Casandra
    query = stream.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .outputMode('append') \
        .options(table="predictions", keyspace="predictions", checkpointLocation=r'D:\checkpoint') \
        .start() \
        .awaitTermination()

    return query

