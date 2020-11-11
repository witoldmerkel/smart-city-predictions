import platform
import os
import load_table
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import hour, minute, year, month, dayofweek, when, lag, col
from pyspark.sql.window import Window

# Załadowanie i przetworzenie danych z tabeli powietrze


def load_powietrze(keys_space_name="json", table_name="powietrze"):

    # Wczytanie danych

    powietrze_temp, sc = load_table.load_and_get_table_df(keys_space_name, table_name)

    # Dodanie zmiennych opisujących dokładnie czas

    powietrze_temp = powietrze_temp.withColumn("normal_type", powietrze_temp["timestamp"].cast(TimestampType()))
    godzina = powietrze_temp.withColumn('godzina', hour(powietrze_temp['normal_type']))
    minuta = godzina.withColumn('minuta', minute(godzina['normal_type']))
    rok = minuta.withColumn('rok', year(minuta['normal_type']))
    miesiac = rok.withColumn('miesiac', month(rok['normal_type']))
    powietrze = miesiac.withColumn("dzien", dayofweek(miesiac["normal_type"]))

    # Stworzenie targeta

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

    dane.show()

    # Zamkniecie polaczenia ze Spark

    sc.stop()

    plt = platform.system()

    if plt == "Windows":
        os.system('rmdir /q /s "D:\SparkTEMP"')

    return dane, sc

