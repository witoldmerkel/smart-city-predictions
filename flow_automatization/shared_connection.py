from speed_layer import kafka_spark_connection


def prepare_sk_connection(list_of_sources, spark):
    topics = []
    for source in list_of_sources:
        if source == "powietrze":
            topics.append("sparkpowietrze")

        elif source == "urzedy":
            topics.append("sparkurzedy")

        elif source == "velib":
            topics.append("sparkvelib")

    topics_join = ",".join(topics)

    sc, spark = kafka_spark_connection.create_sk_connection(topics_join, spark)

    return sc, spark, topics
