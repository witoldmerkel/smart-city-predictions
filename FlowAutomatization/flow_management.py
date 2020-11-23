import SpeedLayer.speed_connection
import os
import findspark
import spark_ml.classificator.Classification
import spark_ml.reggresor.Regression
import Data_for_ML.powietrze_manipulation
import Data_for_ML.urzedy_manipulation
import Data_for_ML.velib_manipulation
from functions_wrapers import load_and_train
import time

# Funkcja automatyzująca cykliczne wykonywania pobierania danych i uczenia modeli oraz ciągłego wykonywania predykcji


def start_flow(list_of_sources=["velib", "powietrze", "urzedy"], refresh_time=86400):

    # Inicjalizacja pierwszych modeli
    for source in list_of_sources:
        load_and_train(source)
        time.sleep(5)



start_flow(list_of_sources=["powietrze", "urzedy"])
