from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import IndexToString, OneHotEncoder, StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from cassandra.cluster import Cluster
from time import time
import platform
import os


def make_class_model(data, sc, model_path, model_name, target, ml_model='default', save=True):

    t0 = time()
    # Stages for pipline
    stages = []

    # Index labels, adding metadata to the label column.
    # Fit on whole dataset to include all labels in index.
    targetIndexer = StringIndexer(inputCol="target", outputCol="indexedTarget", handleInvalid="keep").fit(data)
    stages += [targetIndexer]

    # Split the data into training and test sets (30% held out for testing)
    (trainingData, testData) = data.randomSplit([0.7, 0.3])

    # Identify categorical and numerical variables
    catCols = [x for (x, dataType) in trainingData.dtypes if (((dataType == "string") | (dataType == "boolean"))
               & (x != "target"))]

    numCols = [x for (x, dataType) in trainingData.dtypes if ((dataType == "int") | (dataType == "bigint")
                                                                 | (dataType == "float") | (dataType == "double"))]

    # OneHotEncode categorical variables
    indexers = [StringIndexer(inputCol=column, outputCol=column + "-index", handleInvalid="keep") for column in catCols]

    encoder = OneHotEncoder(
        inputCols=[indexer.getOutputCol() for indexer in indexers],
        outputCols=["{0}-encoded".format(indexer.getOutputCol()) for indexer in indexers]
    )
    assembler_cat = VectorAssembler(
        inputCols=encoder.getOutputCols(),
        outputCol="categorical-features",
        handleInvalid="skip"
    )

    stages += indexers
    stages += [encoder, assembler_cat]



    assembler_num = VectorAssembler(
        inputCols=numCols,
        outputCol="numerical-features",
        handleInvalid="skip"
    )

    # Standardize numerical variables
    scaler = StandardScaler(inputCol="numerical-features", outputCol="numerical-features_scaled")

    # Combine all features in one vector
    assembler_all = VectorAssembler(
        inputCols=['categorical-features', 'numerical-features_scaled'],
        outputCol='features',
        handleInvalid="skip"
    )

    stages += [assembler_num, scaler, assembler_all]

    # Train a RandomForest model by default or another specified model.
    if ml_model == 'default':
        rf = RandomForestClassifier(labelCol="indexedTarget", featuresCol="features", numTrees=10)
    else:
        rf = ml_model

    # Convert indexed labels back to original labels.
    labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel",
                                   labels=targetIndexer.labels)

    stages += [rf, labelConverter]

    # Chain indexers and forest in a Pipeline
    pipeline = Pipeline(stages=stages)

    # Train model.  This also runs the indexers.
    model = pipeline.fit(trainingData)

    # Make predictions.
    predictions = model.transform(testData)

    # Select example rows to display.
    #predictions.select("predictedLabel", "target", "features").show(5)

    # Select (prediction, true label) and compute test error
    evaluator = MulticlassClassificationEvaluator(
        labelCol="indexedTarget", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    print("Accuracy = %g" % (0.0 + accuracy))

    if save:
        # Final model saving and statistics writing
        tt = time() - t0
        timestamp = int(time())
        model.write().overwrite().save(model_path)

        cluster = Cluster(['127.0.0.1'], "9042")
        session = cluster.connect("models")
        query = ("INSERT INTO %s (model_name, timestamp, target, learning_time, model_path, stat)") % ("models_statistics")
        query = query + " VALUES (%s, %s, %s, %s, %s, %s)"
        session.execute(query, (model_name, timestamp, target, tt, model_path, accuracy))
        session.shutdown()
        cluster.shutdown()

        # Stop spark session
        sc.stop()

    if not save:
        return model, sc

    # Deleting temp files for Windows systems
    #plt = platform.system()

    # if plt == "Windows":
        # os.system('rmdir /q /s "D:\SparkTEMP"')
