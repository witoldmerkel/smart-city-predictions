from pyspark.ml import PipelineModel
import pyspark.sql.functions as F

def stream_to_predictions(stream, model_path, target):
    stream = stream.withColumn("target_column", F.lit(target))
    stream = stream.withColumn("model_path", F.lit(model_path))
    loaded_model = PipelineModel.load(model_path)
    stream = loaded_model.transform(stream)
    stream = stream.withColumn("predictedlabel", stream["predictedLabel"])
    return stream







