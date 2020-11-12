import os
import platform
import load_table
import common_manipulations
from pyspark.sql.functions import lead
from pyspark.sql.window import Window
from pyspark.sql.types import BooleanType

# Załadowanie i przetworzenie danych z tabeli velib


def load_velib(keys_space_name="json", table_name="velib"):

    # Wczytanie danych

    velib_temp, sc = load_table.load_and_get_table_df(keys_space_name, table_name)

    # Dodanie zmiennych opisujących dokładnie czas

    velib = common_manipulations.timestamp_to_date(velib_temp)

    # Stworzenie zmiennej celu

    w = Window().partitionBy("station_id").orderBy("timestamp")
    dane = velib.withColumn("target", lead("num_bikes_available", 240).over(w)).na.drop()

    # Usuniecie kolumn nieuzywanych do predykcji
    dane = dane.drop(*['normal_type', 'numbikesavailable', 'numdocksavailable', 'station_code'])
    dane = dane.withColumn("is_installed", dane['is_installed'].cast(BooleanType())) \
        .withColumn('is_renting', dane['is_renting'].cast(BooleanType())) \
        .withColumn('is_returning', dane['is_returning'].cast(BooleanType()))

    dane.sort("station_id", "timestamp").show(300)
    print(dane.dtypes)

    # Zamkniecie polaczenia ze Spark

    sc.stop()

    plt = platform.system()

    if plt == "Windows":
        os.system('rmdir /q /s "D:\SparkTEMP"')

    return dane, sc

dane = load_velib()