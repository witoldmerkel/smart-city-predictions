import load_table
import common_manipulations
from pyspark.sql.functions import when, lead
from pyspark.sql.window import Window


# Załadowanie i przetworzenie danych z tabeli powietrze
def powietrze_preprocessing(pow_data):

    powietrze = common_manipulations.timestamp_to_date(pow_data)

    powietrze = powietrze.withColumn("target_temp", (when(powietrze["pm25"] < 12, "Dobre")
                                                     .when(powietrze["pm25"] <= 35, "Umiarkowane")
                                                     .when(powietrze["pm25"] <= 55, "Niezdrowe dla chorych")
                                                     .when(powietrze["pm25"] <= 150, "Niezdrowe")
                                                     .when(powietrze["pm25"] <= 250, "Bardzo niezdrowe")
                                                     .otherwise("Niebezpieczne")))

    dane = powietrze.drop(*['tz', 'normal_type', 'minuta'])

    return dane


def load_powietrze(keys_space_name="json", table_name="powietrze"):

    # Wczytanie danych

    powietrze_temp, sc = load_table.load_and_get_table_df(keys_space_name, table_name)

    # Dodanie zmiennych opisujących dokładnie czas i suniecie kolumn nieuzywanych do predykcji

    powietrze = powietrze_preprocessing(powietrze_temp)

    # Stworzenie zmiennej celu

    w = Window().partitionBy("name").orderBy("timestamp")
    dane = powietrze.withColumn("target", lead("target_temp", 4).over(w)).na.drop()

    dane.sort("name", "timestamp").show(200)
    print(dane.dtypes)

    return dane, sc

