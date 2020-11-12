from pyspark.sql.functions import hour, minute, year, month, dayofweek
from pyspark.sql.types import TimestampType, StringType


def timestamp_to_date(data):
    data = data.withColumn("normal_type", data["timestamp"].cast(TimestampType()))
    godzina = data.withColumn('godzina', hour(data['normal_type']).cast(StringType()))
    minuta = godzina.withColumn('minuta', minute(godzina['normal_type']).cast(StringType()))
    rok = minuta.withColumn('rok', year(minuta['normal_type']).cast(StringType()))
    miesiac = rok.withColumn('miesiac', month(rok['normal_type']).cast(StringType()))
    data = miesiac.withColumn("dzien", dayofweek(miesiac["normal_type"]).cast(StringType()))
    return data
