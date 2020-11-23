from functions_wrapers import load_and_train, activate_stream
import time

# Funkcja automatyzująca cykliczne wykonywania pobierania danych i uczenia modeli oraz ciągłego wykonywania predykcji


def start_flow(list_of_sources=["velib", "powietrze", "urzedy"], refresh_time=86400):

    # Inicjalizacja pierwszych modeli
    for source in list_of_sources:
        load_and_train(source)
        time.sleep(10)

    while True:
        connections = []
        for source in list_of_sources:
            query, spark = activate_stream(source)
            time.sleep(10)
            connections.append([query, spark])
        time.sleep(refresh_time)

        for connection in connections:
            query, spark = connection
            query.stop()
            spark.stop()
            time.sleep(10)

        for source in list_of_sources:
            load_and_train(source)
            time.sleep(10)






start_flow(list_of_sources=["powietrze", "urzedy"])
