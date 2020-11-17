import load_table
import common_manipulations
from pyspark.sql.functions import lead
from pyspark.sql.window import Window


# Załadowanie i przetworzenie danych z tabeli urzedy

def urzedy_preprocessing(urzedy_data):

    urzedy_temp = urzedy_data.drop(*['czasobslugi', 'l_p', 'status', 'aktualny_numer'])
    urzedy = common_manipulations.timestamp_to_date(urzedy_temp)

    dane = urzedy.drop(*['normal_type'])


    return dane


def load_urzedy(keys_space_name="json", table_name="urzedy"):

    # Wczytanie danych
    
    urzedy_temp, sc = load_table.load_and_get_table_df(keys_space_name, table_name)

    urzedy = urzedy_preprocessing(urzedy_temp)
    
    # Stworzenie zmiennej celu

    w = Window().partitionBy("idgrupy").orderBy("timestamp")
    dane = urzedy.withColumn("target", lead("liczbaklwkolejce", 240).over(w)).na.drop()

    dane.sort("idgrupy", "timestamp").show(300)
    print(dane.dtypes)

    return dane, sc
