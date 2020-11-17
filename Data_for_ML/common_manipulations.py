from pyspark.sql.functions import hour, minute, dayofweek
from pyspark.sql.types import TimestampType, StringType


def timestamp_to_date(data):
    data = data.withColumn("normal_type", data["timestamp"].cast(TimestampType()))
    godzina = data.withColumn('godzina', hour(data['normal_type']).cast(StringType()))
    minuta = godzina.withColumn('minuta', minute(godzina['normal_type']).cast(StringType()))
    data = minuta.withColumn("dzien", dayofweek(minuta["normal_type"]).cast(StringType()))
    return data
