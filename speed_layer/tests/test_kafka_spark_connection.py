from kafka_spark_connection import create_sk_connection
import pytest


# Test sprawdzający poprawność nawiązaywania połączeń na lini spark-kafka
@pytest.mark.parametrize('topic', [
    ('sparkvelib'
     )])
def test_create_sk_connection(topic):
    stream, spark = create_sk_connection(topic)
    actual_schema = str(stream)
    expected_schema = "DataFrame[key: binary, value: binary, topic: string, partition: int, offset: bigint," \
                      " timestamp: timestamp, timestampType: int]"
    assert actual_schema == expected_schema
    spark.stop()
