from functions_wrapers import load_and_train
import time


# Funkcja automatyzująca cykliczne wykonywania pobierania danych i uczenia modeli


def start_flow_learning(list_of_sources=["velib", "powietrze", "urzedy"], refresh_time=86400):

    # Okresowe ładowanie danych oraz uczenie modeli
    while True:

        for source in list_of_sources:
            load_and_train(source)
            time.sleep(15)

        time.sleep(refresh_time)


if __name__ == "__main__":
    start_flow_learning(list_of_sources=["powietrze", "urzedy", "velib"])
