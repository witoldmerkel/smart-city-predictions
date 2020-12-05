from streams_handling import stream_to_predictions
import pytest


# Test sprawdzający poprawność automatycznego tworzenia predykcji na podstawie zwracanego schematu danych
@pytest.mark.parametrize('target, source_name', [
    ('test_target',
     'test_source'
     )])
def test_stream_to_predictions(stream, model_path, target, source_name):
    stream, _ = stream
    predictions = stream_to_predictions(stream, model_path, target, source_name)
    actual_schema = str(predictions)
    expected_schema = "DataFrame[target: string, v1: double, v2: double, v3: double, v4: double, v5: string," \
                      " target_column: string, model_path: string, source_name: string, indexedTarget: double," \
                      " v5-index: double, v5-index-encoded: vector, categorical-features: vector," \
                      " numerical-features: vector, numerical-features_scaled: vector," \
                      " features: vector, rawPrediction: vector, probability: vector," \
                      " prediction: string, predictedLabel: string]"
    assert actual_schema == expected_schema

