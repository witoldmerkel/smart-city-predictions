from chispa.dataframe_comparer import assert_df_equality
import common_manipulations
from datetime import datetime


# Testowanie czy tabela zwracana jest zgodna z tabelą oczekiwaną
def test_timestamp_to_date(spark):
    source_data = [
        (1606946341, "a"),
        (1606946341, "a")
    ]
    source_df = spark.createDataFrame(source_data, ["timestamp", "name"])
    actual_df = common_manipulations.timestamp_to_date(source_df)
    expected_data = [
        (1606946341, "a", datetime.fromtimestamp(1606946341), "22", "59", "4"),
        (1606946341, "a", datetime.fromtimestamp(1606946341), "22", "59", "4")
    ]
    expected_df = spark.createDataFrame(expected_data, ["timestamp",  "name", "normal_type", "godzina", "minuta",
                                                        "dzien"])
    assert_df_equality(actual_df, expected_df)
