from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import IndexToString, OneHotEncoder, StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import platform
import os


def make_class_model(data, sc):

    # Stages for pipline
    stages = []

    # Index labels, adding metadata to the label column.
    # Fit on whole dataset to include all labels in index.
    targetIndexer = StringIndexer(inputCol="target", outputCol="indexedTarget").fit(data)
    stages += [targetIndexer]

    # Split the data into training and test sets (30% held out for testing)
    (trainingData, testData) = data.randomSplit([0.7, 0.3])

    # Identify categorical and numerical variables
    catCols = [x for (x, dataType) in trainingData.dataTypes if (((dataType == "string") | (dataType == "boolean"))
               & (x != "target"))]

    numCols = [x for (x, dataType) in trainingData.dataTypes if ((dataType == "int") | (dataType == "bigint")
                                                                 | (dataType == "float") | (dataType == "double"))]

    # OneHotEncode categorical variables
    indexers = [StringIndexer(inputCol=column, outputCol=column + "-index") for column in catCols]

    encoder = OneHotEncoder(
        inputCols=[indexer.getOutputCol() for indexer in indexers],
        outputCols=["{0}-encoded".format(indexer.getOutputCol()) for indexer in indexers]
    )
    assembler_cat = VectorAssembler(
        inputCols=encoder.getOutputCols(),
        outputCol="categorical-features"
    )

    stages += [indexers, encoder, assembler_cat]

    # Standardize numerical variables
    scalers = [StandardScaler(inputCol=column, outputCol=column + "-scaled") for column in numCols]

    assembler_num = VectorAssembler(
        inputCols=[scaler.getOutputCol() for scaler in scalers],
        outputCol="numerical-features"
    )

    # Combine all features in one vector
    assembler_all = VectorAssembler(
        inputCols=['categorical-features', 'numerical-features'],
        outputCol='features'
    )

    stages += [scalers, assembler_num, assembler_all]

    # Train a RandomForest model.
    rf = RandomForestClassifier(labelCol="indexedTarget", featuresCol="features", numTrees=10)

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
    predictions.select("predictedLabel", "label", "features").show(5)

    # Select (prediction, true label) and compute test error
    evaluator = MulticlassClassificationEvaluator(
        labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    print("Accuracy = %g" % (0.0 + accuracy))

    rfModel = model.stages[2]
    print(rfModel)

    # Stop spark session
    sc.stop()

    # Deleting temp files for Windows systems
    plt = platform.system()

    if plt == "Windows":
        os.system('rmdir /q /s "D:\SparkTEMP"')

    return model
