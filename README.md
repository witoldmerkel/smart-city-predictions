# smart-city-predictions
System prognostyczny dedykowany dla danych inteligentnych miast

Praca inżynierska realizowana przez Michała Stawikowskiego and Witolda Merkela

## Abstrakt

Celem pracy było zaprojektowanie i realizacja systemu informatycznego, który wykorzy-stuje środowiska składowania i przetwarzania danych wielkoskalowych (ang. Big Data) dopozyskiwania strumieni danych z inteligentnych miast (ang. Smart City) oraz metody uczeniamaszynowego do prognozowania na podstawie tych danych. System powinien mieć otwartąarchitekturę, która umożliwia dołączanie nowych źródeł danych oraz dołączanie nowychkomponentów, które tworzą zbiory uczące i testowe na potrzeby uczenia modeli klasyfikacyjnychi regresyjnych oraz wykonują prognozy z użyciem tych modeli. Postawione cele zostały zreali-zowane. W ramach systemu zostały zaimplementowane przykładowe komponenty pozyskiwaniadanych z różnych źródeł danych oraz ich składowanie, wykorzystujące uznane platformy BigData. Dodatkowo zostały stworzone przykładowe komponenty, które na podstawie zgroma-dzonych danych wykonują proces uczenia modeli klasyfikacyjnych i regresyjnych, a następniewyznaczają i udostępniają prognozowane wartości oraz statystyki uczenia modeli. W celuprezentacji informacji oraz wyników działania systemu zaimplementowano graficzny interfejsużytkownika. Na pracę składa się dogłębna analiza problemu, przedstawienie procesu projekto-wania systemu, opis działania stworzonych modułów, a także dokładna dokumentacja techniczna.

## Przewodnik po repozytorium
* data_for_ml - folder zawierający podstawowe operacje na danych. Funkcje zawarte w tym folderze służą przygotowaniu danych do uczenia maszynowego.
* flask-with-auth - folder zawierający część aplikacji odpowiedzialną na graficzny interfejs użytkownika. Tutaj znajduje się baza danych użytkowników, kody .html, .css i .js odpowiedzialne za zarzadzanie poszczególnymi stronami oraz serwer w Flask.
* flow_authomatization - folder zawierający funkcje odpowiedzialne za zarządzanie procesem trenowania modeli uczenia maszynowego oraz predykcji.
* nifi - folder zawierający schematy wykorzystywanych przepływów w Apache NiFi.
* spark_ml - zawiera funkcje tworzące modele regresyjne jak i klasyfikatory oraz dokunujące predykcji.
* speed_layer - zawiera funkcje zarządzające przetwarzaniem strumieniowym oraz zapisem predykcji do Apache Cassandra.
