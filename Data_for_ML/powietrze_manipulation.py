import platform
import os
import load_table, common_manipulations
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import hour, minute, year, month, dayofweek, when, lag, col
from pyspark.sql.window import Window

# Załadowanie i przetworzenie danych z tabeli powietrze


def load_powietrze(keys_space_name="json", table_name="powietrze"):

    # Wczytanie danych

    powietrze_temp, sc = load_table.load_and_get_table_df(keys_space_name, table_name)

    # Dodanie zmiennych opisujących dokładnie czas

    powietrze = common_manipulations.timestamp_to_date(powietrze_temp)

    # Stworzenie zmiennej celu

    powietrze = powietrze.withColumn("target", (when(powietrze["pm25"] < 12, "Dobre").when(powietrze["pm25"] <= 35, "Umiarkowane")
                                                .when(powietrze["pm25"] <= 55, "Niezdrowe dla chorych").when(powietrze["pm25"] <= 150, "Niezdrowe")
                                                .when(powietrze["pm25"] <= 250, "Bardzo niezdrowe").otherwise("Niebezpieczne")))

    iterator1 = powietrze.select('name').distinct().orderBy(powietrze['name'])
    iterator = iterator1.toPandas()
    num = 0

    w = Window().partitionBy().orderBy(col("timestamp"))
    for id in iterator['name']:
        temp = powietrze.where(powietrze['name'] == id)
        temp = temp.orderBy(temp["timestamp"])
        temp = temp.select("*", lag("target", 24).over(w).alias("target(stan_za_4h)")).na.drop()
        if num == 0:
            dane = temp
        else:
            dane = dane.union(temp)
        num+=1

    dane.show(100)
    print(dane.dtypes)
    # Zamkniecie polaczenia ze Spark

    sc.stop()

    plt = platform.system()

    if plt == "Windows":
        os.system('rmdir /q /s "D:\SparkTEMP"')

    return dane, sc

dane = load_powietrze()


