from Classification import make_class_model
import findspark
import os
from time import time
import pytest


def test_make_class_model_pipeline(spark_df_class):
    spark, df = spark_df_class
    path = findspark.find()
    path = os.path.join(path, 'models')
    test_path = os.path.join(path, 'test_model')
    test_path = test_path + '_' + str(int(time()))
    model, _ = make_class_model(df, spark, test_path, "test_class_model", "test_target",
                                                              save=False)
    actual_stages = str([type(x) for x in model.stages])
    expected_stages = "[<class 'pyspark.ml.feature.StringIndexerModel'>," \
                      " <class 'pyspark.ml.feature.StringIndexerModel'>," \
                      " <class 'pyspark.ml.feature.OneHotEncoderModel'>," \
                      " <class 'pyspark.ml.feature.VectorAssembler'>," \
                      " <class 'pyspark.ml.feature.VectorAssembler'>," \
                      " <class 'pyspark.ml.feature.StandardScalerModel'>," \
                      " <class 'pyspark.ml.feature.VectorAssembler'>," \
                      " <class 'pyspark.ml.classification.RandomForestClassificationModel'>," \
                      " <class 'pyspark.ml.feature.IndexToString'>]"
    assert actual_stages == expected_stages


