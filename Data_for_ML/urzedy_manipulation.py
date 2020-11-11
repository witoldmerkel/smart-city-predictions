import platform
import os
import load_table
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import hour, minute, year, month, dayofweek, when, lag, col
from pyspark.sql.window import Window
import platform

# Załadowanie i przetworzenie danych z tabeli urzedy


def load_urzedy(keys_space_name="json", table_name="urzedy"):

    # Wczytanie danych
    
    urzedy_temp, sc = load_table.load_and_get_table_df(keys_space_name, table_name)
    urzedy_temp = urzedy_temp.drop(*['czasobslugi', 'l_p', 'status', 'aktualny_numer'])
    
    # Dodanie zmiennych opisujących dokładnie czas
    
    urzedy_temp = urzedy_temp.withColumn("normal_type", urzedy_temp["timestamp"].cast(TimestampType()))
    godzina = urzedy_temp.withColumn('godzina', hour(urzedy_temp['normal_type']))
    minuta = godzina.withColumn('minuta', minute(godzina['normal_type']))
    rok = minuta.withColumn('rok', year(minuta['normal_type']))
    miesiac = rok.withColumn('miesiac', month(rok['normal_type']))
    urzedy = miesiac.withColumn("dzien", dayofweek(miesiac["normal_type"]))
    
    # Stworzenie zmiennej celu
    
    iterator1 = urzedy.select('idgrupy').distinct().orderBy(urzedy['idgrupy'])
    iterator = iterator1.toPandas()
    num = 0
    
    w = Window().partitionBy().orderBy(col("timestamp"))
    for id in iterator['idgrupy']:
        temp = urzedy.where(urzedy['idgrupy'] == id)
        temp = temp.orderBy(temp["timestamp"])
        temp = temp.select("*", lag("liczbaklwkolejce", 240).over(w).alias("liczbaklwkolejce(stan_za_4h)")).na.drop()
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
