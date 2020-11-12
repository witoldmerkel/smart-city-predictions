from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql import SparkSession


def make_class_model(data):

    # Split the data into training and test sets (30% held out for testing)
    (trainingData, testData) = data.randomSplit([0.7, 0.3])

    catCols = [x for (x, dataType) in trainingData.dataTypes if ((dataType == "string") | (dataType == "boolean"))]

    numCols = [x for (x, dataType) in trainingData.dataTypes if ((dataType == "int") | (dataType == "bigint")
                                                                 | (dataType == "float"))]
