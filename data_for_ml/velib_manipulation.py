import load_table
import common_manipulations
from pyspark.sql.functions import lead
from pyspark.sql.window import Window
from pyspark.sql.types import StringType

# Załadowanie i przetworzenie danych z tabeli velib


def velib_preprocessing(velib_data, agg, row_start, row_end):
    dane = common_manipulations.timestamp_to_date(velib_data)

    dane = dane.drop(*['normal_type', 'numbikesavailable', 'numdocksavailable', 'station_code'])
    dane = dane.withColumn("is_installed", dane['is_installed'].cast(StringType())) \
        .withColumn('is_renting', dane['is_renting'].cast(StringType())) \
        .withColumn('is_returning', dane['is_returning'].cast(StringType())) \
        .withColumn('station_id', dane['station_id'].cast(StringType())).na.drop()

    if agg == "moving_average":
        dane = common_manipulations.moving_average_aggregation(dane, "num_bikes_available", "station_id", row_start,
                                                                 row_end)
    else:
        print("No aggregation")

    return dane


def load_velib(keys_space_name="json", table_name="velib", time_frame=None, spark=None, agg=None,
                row_start=-1, row_end=1):

    # Wczytanie danych

    velib_temp, sc = load_table.load_and_get_table_df(keys_space_name, table_name, time_frame, spark)

    # Dodanie zmiennych opisujących dokładnie czas i Usuniecie kolumn nieuzywanych do predykcji

    velib = velib_preprocessing(velib_temp, agg, row_start, row_end)


    velib.sort("station_id", "timestamp").show(300)

    # Stworzenie zmiennej celu

    w = Window().partitionBy("station_id").orderBy("timestamp")
    dane = velib.withColumn("target", lead("num_bikes_available", 240).over(w)).na.drop()

    #dane.sort("station_id", "timestamp").show(300)
    #print(dane.dtypes)

    return dane, sc
