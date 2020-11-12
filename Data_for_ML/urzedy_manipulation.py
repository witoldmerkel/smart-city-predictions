import os
import load_table
import common_manipulations
from pyspark.sql.functions import lead
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

    w = Window().partitionBy("idgrupy").orderBy("timestamp")
    dane = urzedy.withColumn("target", lead("liczbaklwkolejce", 240).over(w)).na.drop()

    # Usuniecie kolumn nieuzywanych do predykcji
    dane = dane.drop(*['normal_type'])
    dane.sort("idgrupy", "timestamp").show(300)
    print(dane.dtypes)

    
    # Zamkniecie polaczenia ze Spark
    
    sc.stop()
    
    plt = platform.system()
    
    if plt == "Windows":
        os.system('rmdir /q /s "D:\SparkTEMP"')

    return dane, sc

dane = load_urzedy()