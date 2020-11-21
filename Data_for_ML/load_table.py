import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
import calendar
import time
# W tym pliku znajdują się funkcje odpowiedzialne za tworzenie połączenia oraz wczytywanie tabel z bazy danych Cassandra


def load_and_get_table_df(keys_space_name, table_name, time_frame=None):
    # WYmagane pakiety oraz intefejsy
    os.environ[
        'PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.0.0' \
                                 ' --conf spark.cassandra.connection.host=127.0.0.1 pyspark-shell'
    # Tworzenie połączenia spark-cassandra
    spark = SparkSession.builder.getOrCreate()
    table_df = spark.read\
        .format("org.apache.spark.sql.cassandra")\
        .options(table=table_name, keyspace=keys_space_name)\
        .load()

    if time_frame is None:
        time_frame = calendar.timegm(time.gmtime()) - 1209600
    table_df = table_df.filter(f.col("timestamp") > time_frame)

    return table_df, spark
