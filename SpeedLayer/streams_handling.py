from pyspark.ml import PipelineModel
import pyspark.sql.functions as F
# W tym pliku znajdują się funckje, które są odpowiedzialne za określone transformacje strumieni danych

def stream_to_predictions(stream, model_path, target):
    # Stworzenie dodatkowych kolumn w strumieniu
    stream = stream.withColumn("target_column", F.lit(target))
    stream = stream.withColumn("model_path", F.lit(model_path))
    # Załadowanie nauczonego modelu
    loaded_model = PipelineModel.load(model_path)
    # Wygenerowanie strumienia predykcji na podstawie strumienia danych
    stream = loaded_model.transform(stream)
    # Zmiana nazwy kolumn w celu zachowania spójności z nazewnictwem kolumn w bazie danych Cassandra
    if "predictedLabel" in stream.columns:
        stream = stream.withColumn("predictedlabel", stream["predictedLabel"])
    return stream







