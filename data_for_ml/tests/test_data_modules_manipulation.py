import powietrze_manipulation
import urzedy_manipulation
import velib_manipulation
import pytest
from datetime import datetime
from chispa.dataframe_comparer import assert_df_equality


# Testowanie czy tabela zwracana jest zgodna z tabelą oczekiwaną
def test_powietrze_preprocessing(spark):

    source_data = [
        (1606946341, "a", datetime.fromtimestamp(1606946341), "22", "59", "4", 55, "+1"),
        (1606946341, "a", datetime.fromtimestamp(1606946341), "22", "59", "4", 150, "+2")
    ]
    source_df = spark.createDataFrame(source_data, ["timestamp", "name", "normal_type", "godzina", "minuta",
                                                        "dzien", "pm25", "tz"])

    actual_df = powietrze_manipulation.powietrze_preprocessing(source_df)

    expected_data = [
        (1606946341, "a", "22", "4", 55, "Niezdrowe dla chorych"),
        (1606946341, "a", "22", "4", 150, "Niezdrowe")
    ]

    expected_df = spark.createDataFrame(expected_data, ["timestamp", "name", "godzina",
                                                        "dzien", "pm25", "target_temp"])

    expected_df.schema['target_temp'].nullable = False

    assert_df_equality(actual_df, expected_df)


def test_urzedy_preprocessing(spark):

    source_data = [
        (1606946341, "a", datetime.fromtimestamp(1606946341), "22", "59", "4"),
        (1606946341, "a", datetime.fromtimestamp(1606946341), "22", "59", "4")
    ]
    source_df = spark.createDataFrame(source_data, ["timestamp", "name", "normal_type", "godzina", "minuta",
                                                        "dzien"])

    actual_df = urzedy_manipulation.urzedy_preprocessing(source_df)

    expected_data = [
        (1606946341, "a", "22", "59", "4"),
        (1606946341, "a", "22", "59", "4")
    ]

    expected_df = spark.createDataFrame(expected_data, ["timestamp", "name", "godzina", "minuta",
                                                        "dzien"])

    assert_df_equality(actual_df, expected_df)


def test_velib_preprocessing(spark):

    source_data = [
        (1606946341, "a", datetime.fromtimestamp(1606946341), "22", "59", "4"),
        (1606946341, "a", datetime.fromtimestamp(1606946341), "22", "59", "4")
    ]
    source_df = spark.createDataFrame(source_data, ["timestamp", "name", "normal_type", "godzina", "minuta",
                                                        "dzien"])

    actual_df = urzedy_manipulation.urzedy_preprocessing(source_df)

    expected_data = [
        (1606946341, "a", "22", "59", "4"),
        (1606946341, "a", "22", "59", "4")
    ]

    expected_df = spark.createDataFrame(expected_data, ["timestamp", "name", "godzina", "minuta",
                                                        "dzien"])

    assert_df_equality(actual_df, expected_df)

# Testowanie czy schemat tabeli zwracanej jest zgodny z zakładanym schematem co potwierdza pomyślne połączenie z bazą
# danych, test parametryzowany

@pytest.mark.parametrize('keys_space_name, table_name, expected_schema, function_load', [
    ('json',
     'powietrze',
     "StructType(List(StructField(name,StringType,false),StructField(timestamp,LongType,true),"
     "StructField(co,FloatType,true),StructField(h,FloatType,true),StructField(lat,FloatType,true),"
     "StructField(long,FloatType,true),StructField(no2,FloatType,true),StructField(o3,FloatType,true),"
     "StructField(p,FloatType,true),StructField(pm10,FloatType,true),StructField(pm25,FloatType,true),"
     "StructField(so2,FloatType,true),StructField(t,FloatType,true),StructField(w,FloatType,true),"
     "StructField(godzina,StringType,true),StructField(dzien,StringType,true),"
     "StructField(target_temp,StringType,false),StructField(target,StringType,true)))",
     powietrze_manipulation.load_powietrze
     )])
def test_load_modules(keys_space_name, table_name, expected_schema, function_load):
    table_df, sc = function_load(keys_space_name, table_name)
    actual_schema = str(table_df.schema)
    assert actual_schema == expected_schema
    sc.stop()






