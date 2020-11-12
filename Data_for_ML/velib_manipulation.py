import os
import platform
import load_table, common_manipulations
from pyspark.sql.functions import lag, col
from pyspark.sql.window import Window

# Załadowanie i przetworzenie danych z tabeli velib


def load_velib(keys_space_name="json", table_name="velib"):

    # Wczytanie danych

    velib_temp, sc = load_table.load_and_get_table_df(keys_space_name, table_name)

    # Dodanie zmiennych opisujących dokładnie czas

    velib = common_manipulations.timestamp_to_date(velib_temp)

    # Stworzenie zmiennej celu

    iterator1 = velib.select('station_id').distinct().orderBy(velib['station_id'])
    iterator = iterator1.toPandas()
    num = 0

    w = Window().partitionBy().orderBy(col("timestamp"))
    for id in iterator['station_id']:
        temp = velib.where(velib['station_id'] == id)
        temp = temp.orderBy(temp["timestamp"])
        temp = temp.select("*", lag("num_bikes_available", 240).over(w).alias("num_bikes_available(stan_za_4h)")).na.drop()
        if num == 0:
            dane = temp
        else:
            dane = dane.union(temp)
        num+=1

    dane.show()

    # Zamkniecie polaczenia ze Spark

    sc.stop()

    plt = platform.system()

    if plt == "Windows":
        os.system('rmdir /q /s "D:\SparkTEMP"')

    return dane, sc