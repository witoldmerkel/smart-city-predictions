# smart-city-predictions
Prediction system for smart city data.

Bachelor's thesis by Michał Stawikowski and Witold Merkel

## Abstract

The goal of this thesis is to design and implement an IT system that uses the Big Data storage and processing environments to obtain data streams from smart cities, and machine learning methods for forecasting based on this data. The system should have an open architecture that allows adding new data sources and new components that create training and test datasets for training classification and regression models and make predictions using these models. The set goals have been achieved. As part of the system, sample components of data acquisition from various data sources and databases using recognized Big Data platforms have been implemented. Additionally, exemplary components have been created which, based on the collected data, perform the process of learning classification and regression models, and then use them to calculate and provide predicted values and model learning statistics. A graphical user interface has been implemented to present information and the results of the system operations. The thesis consists of an in-depth problem analysis, presentation of the system design process, and description of the created modules, as well as detailed technical documentation of the work performed.

## Przewodnik po repozytorium
* data_for_ml - folder zawierający podstawowe operacje na danych. Funkcje zawarte w tym folderze służą przygotowaniu danych do uczenia maszynowego.
* flask-with-auth - folder zawierający część aplikacji odpowiedzialną na graficzny interfejs użytkownika. Tutaj znajduje się baza danych użytkowników, kody .html, .css i .js odpowiedzialne za zarzadzanie poszczególnymi stronami oraz serwer w Flask.
* flow_authomatization - folder zawierający funkcje odpowiedzialne za zarządzanie procesem trenowania modeli uczenia maszynowego oraz predykcji.
* nifi - folder zawierający schematy wykorzystywanych przepływów w Apache NiFi.
* spark_ml - zawiera funkcje tworzące modele regresyjne jak i klasyfikatory oraz dokunujące predykcji.
* speed_layer - zawiera funkcje zarządzające przetwarzaniem strumieniowym oraz zapisem predykcji do Apache Cassandra.
* weather - zawiera funkcje dokonujące agregacji dodatkowych danych o pogodzie (funkcjonalność wychodząca poza obszar pracy, któ®ej nie udało się zaimplementować).
