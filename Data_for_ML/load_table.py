import os
from pyspark.sql import SparkSession
# W tym pliku znajdują się funkcje odpowiedzialne za tworzenie połączenia oraz wczytywanie tabel z bazy danych Cassandra


def load_and_get_table_df(keys_space_name, table_name):
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
    return table_df, spark
