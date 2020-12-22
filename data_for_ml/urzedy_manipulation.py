import load_table
import common_manipulations
from pyspark.sql.functions import lead
from pyspark.sql.window import Window


# Załadowanie i przetworzenie danych z tabeli urzedy

def urzedy_preprocessing(urzedy_data, agg=None,
                time_frames="5 minutes", time_update="1 minute"):

    urzedy_temp = urzedy_data.drop(*['czasobslugi', 'l_p', 'status', 'aktualny_numer'])
    urzedy = common_manipulations.timestamp_to_date(urzedy_temp)

    if agg == "moving_average":
        urzedy = common_manipulations.moving_average_aggregation(urzedy, 'liczbaklwkolejce', "idgrupy", time_frames,
                                                                 time_update)
    else:
        print("No aggregation")

    dane = urzedy.drop(*['normal_type']).na.drop()

    return dane


def load_urzedy(keys_space_name="json", table_name="urzedy", time_frame=None, spark=None, agg=None,
                time_frames="5 minutes", time_update="1 minute"):

    # Wczytanie danych
    
    urzedy_temp, sc = load_table.load_and_get_table_df(keys_space_name, table_name, time_frame, spark)

    urzedy = urzedy_preprocessing(urzedy_temp, agg, time_frames, time_update)

    urzedy.sort("idgrupy", "timestamp").show(300)
    
    # Stworzenie zmiennej celu

    w = Window().partitionBy("idgrupy").orderBy("timestamp")
    dane = urzedy.withColumn("target", lead("liczbaklwkolejce", 240).over(w)).na.drop()

    #dane.sort("idgrupy", "timestamp").show(300)
    #print(dane.dtypes)

    return dane, sc
