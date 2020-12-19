import os
import findspark
from time import time
# W tym pliku znajdują się funkcje odpowiedzialne za tworzenie i obsługę połączeń na lini spark-cassadnra


def writeToCassandra(stream, checkpoint="checkpoint"):
    # Tworzenia połączenia spark-cassandra oraz zapisywanie strumienia danych do bazy danych Cassandra

    checkpointLocation = os.path.join(findspark.find(), checkpoint)
    checkpointLocation = os.path.join(checkpointLocation, checkpoint)
    checkpointLocation = checkpointLocation + '_' + str(int(time()))

    query = stream.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .outputMode('append') \
        .options(table="predictions", keyspace="predictions", checkpointLocation=checkpointLocation,
                 failOnDataLoss="false") \
        .start() #\
        #.awaitTermination()

    return query

