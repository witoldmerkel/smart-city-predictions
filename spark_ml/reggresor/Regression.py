from pyspark.ml import Pipeline
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.evaluation import RegressionEvaluator
from time import time
import platform
import os


def make_regr_model(data, sc, model_path, keyspace="", json=""):

    t0 = time()
    # Stages for pipline
    stages = []

    # Split the data into training and test sets (30% held out for testing)
    (trainingData, testData) = data.randomSplit([0.7, 0.3])

    # Identify categorical and numerical variables
    catCols = [x for (x, dataType) in trainingData.dtypes if ((dataType == "string") | (dataType == "boolean"))]

    numCols = [x for (x, dataType) in trainingData.dtypes if (((dataType == "int") | (dataType == "bigint")
                                                                 | (dataType == "float") | (dataType == "double"))
               & (x != "target"))]

    # OneHotEncode categorical variables
    indexers = [StringIndexer(inputCol=column, outputCol=column + "-index", handleInvalid="keep") for column in catCols]

    encoder = OneHotEncoder(
        inputCols=[indexer.getOutputCol() for indexer in indexers],
        outputCols=["{0}-encoded".format(indexer.getOutputCol()) for indexer in indexers]
    )
    assembler_cat = VectorAssembler(
        inputCols=encoder.getOutputCols(),
        outputCol="categorical-features"
    )

    stages += indexers
    stages += [encoder, assembler_cat]



    assembler_num = VectorAssembler(
        inputCols=numCols,
        outputCol="numerical-features"
    )

    # Standardize numerical variables
    scaler = StandardScaler(inputCol="numerical-features", outputCol="numerical-features_scaled")

    # Combine all features in one vector
    assembler_all = VectorAssembler(
        inputCols=['categorical-features', 'numerical-features_scaled'],
        outputCol='features'
    )

    stages += [assembler_num, scaler, assembler_all]

    # Train a RandomForest model.
    rf = RandomForestRegressor(labelCol="target", featuresCol="features")

    stages += [rf]

    # Chain indexers and forest in a Pipeline
    pipeline = Pipeline(stages=stages)

    # Train model.  This also runs the indexers.
    model = pipeline.fit(trainingData)

    # Make predictions.
    predictions = model.transform(testData)

    # Select example rows to display.
    predictions.select("prediction", "target", "features").show(5)

    # Select (prediction, true label) and compute test error
    evaluator = RegressionEvaluator(
        labelCol="target", predictionCol="prediction", metricName="rmse")
    rmse = evaluator.evaluate(predictions)
    print("RMSE = %g" % (0.0 + rmse))

    # Final model saving
    tt = time() - t0
    timestamp = int(time())
    model.write().overwrite().save(model_path)

    # Stop spark session
    sc.stop()

    # Deleting temp files for Windows systems
    plt = platform.system()

    if plt == "Windows":
        os.system('rmdir /q /s "D:\SparkTEMP"')

    return model