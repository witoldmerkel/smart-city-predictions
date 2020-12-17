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


def moving_average_aggregation(data, average_col, partion_col, time_frames="5 minutes", time_update="1 minute"):

    #w = Window().partitionBy(partion_col, F.window("normal_type", "5 minute"))
    #w = Window().partitionBy(partion_col).orderBy("timestamp").rowsBetween(row_start, row_end)
    # W celach demonstracyjnych
    #mov_avg = data.withColumn("moving_average", F.avg(average_col).over(w))

    #mov_avg = data.withColumn(average_col, F.avg(average_col).over(w))

    cols = [x for (x, dataType) in data.dtypes if (x != "normal_type") & (x != average_col) & (x != partion_col)]
    exprs = [F.avg(average_col).alias(average_col)]
    exprs = exprs + [F.first(x).alias(x) for x in cols] + [F.first(average_col)]
    mov_avg = data.withWatermark("normal_type", time_frames)\
        .groupBy(partion_col, F.window("normal_type", time_frames, time_update)).agg(*exprs)

    return mov_avg
