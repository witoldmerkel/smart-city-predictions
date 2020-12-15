from pyspark.sql.functions import hour, minute, dayofweek
from pyspark.sql.types import TimestampType, StringType
import pyspark.sql.functions as F
from pyspark.sql.window import Window
# W tym pliku znajdują się funkcje odpowiedzialne za przeprowadzenie często wykonywanych operacji


def timestamp_to_date(data):
    # Generowanie szczegółowych danych dotyczących czasu na podstawie wartości timestamp
    data = data.withColumn("normal_type", data["timestamp"].cast(TimestampType()))
    godzina = data.withColumn('godzina', hour(data['normal_type']).cast(StringType()))
    minuta = godzina.withColumn('minuta', minute(godzina['normal_type']).cast(StringType()))
    data = minuta.withColumn("dzien", dayofweek(minuta["normal_type"]).cast(StringType()))
    return data


def moving_average_aggregation(data, average_col, partion_col, row_start=-1, row_end=1):

    w = Window().partitionBy(partion_col).orderBy("timestamp").rowsBetween(row_start, row_end)
    # W celach demonstracyjnych
    #mov_avg = data.withColumn("moving_average", F.avg(average_col).over(w))

    mov_avg = data.withColumn(average_col, F.avg(average_col).over(w))

    return mov_avg
