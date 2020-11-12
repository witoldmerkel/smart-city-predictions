import os
import load_table, common_manipulations
from pyspark.sql.functions import lag, col
from pyspark.sql.window import Window
import platform

# Załadowanie i przetworzenie danych z tabeli urzedy


def load_urzedy(keys_space_name="json", table_name="urzedy"):

    # Wczytanie danych
    
    urzedy_temp, sc = load_table.load_and_get_table_df(keys_space_name, table_name)
    urzedy_temp = urzedy_temp.drop(*['czasobslugi', 'l_p', 'status', 'aktualny_numer'])
    
    # Dodanie zmiennych opisujących dokładnie czas
    
    urzedy = common_manipulations.timestamp_to_date(urzedy_temp)
    
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

dane = load_urzedy()