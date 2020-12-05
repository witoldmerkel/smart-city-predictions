from spark_cassandra_connection import writeToCassandra
from cassandra.cluster import Cluster
import findspark
import os
from time import time
from pyspark.sql.types import (
        StructType, StringType, IntegerType
        )


# Test sprawdzający poprawność zapisu predyckji do bazy danych Cassandra
def test_writeToCassandra(predictions, pandas_factory_fixture):

        predictions, spark, timestamp = predictions
        json_path = os.path.join(findspark.find(), "temp_json_test")
        json_file_path = os.path.join(json_path, "test" + str(int(time())))
        predictions.coalesce(1).write.format('json').save(json_file_path)
        schema = StructType() \
                .add("prediction", StringType()) \
                .add("model_path", StringType()) \
                .add("source_name", StringType()) \
                .add("individual", StringType()) \
                .add("target_column", StringType()) \
                .add("timestamp", IntegerType())

        streamingDF = (
                spark
                .readStream
                .schema(schema)
                .option("maxFilesPerTrigger", 6)
                .json(json_file_path)
        )

        connection = writeToCassandra(streamingDF, checkpoint=os.path.join("test_checkpoint",
                                                                           "test" + str(int(time()))))
        connection.awaitTermination(5)

        cluster = Cluster(['127.0.0.1'], "9042")
        session = cluster.connect("predictions")
        session.row_factory = pandas_factory_fixture
        session.default_fetch_size = None
        query_count = "select count(*) from predictions.predictions where target_column = 'test_target'" \
                       " and timestamp = %s ALLOW FILTERING;"
        query_count = query_count % timestamp
        count = session.execute(query_count, timeout=None)._current_rows.iloc[0]['count']
        session.shutdown()
        cluster.shutdown()

        assert streamingDF.isStreaming is True
        assert count > 0
