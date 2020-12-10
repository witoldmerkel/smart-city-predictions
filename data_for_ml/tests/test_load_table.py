import load_table
import pytest


# Testowanie czy schemat tabeli zwracanej jest zgodny z zakładanym schematem co potwierdza pomyślne połączenie z bazą
# danych, test parametryzowany
@pytest.mark.parametrize('keys_space_name, table_name, expected_schema', [
    ('json',
     'powietrze',
     "StructType(List(StructField(name,StringType,false),StructField(timestamp,LongType,true)," \
     "StructField(co,FloatType,true),StructField(h,FloatType,true),StructField(lat,FloatType,true)," \
     "StructField(long,FloatType,true),StructField(no2,FloatType,true)," \
     "StructField(o3,FloatType,true),StructField(p,FloatType,true)," \
     "StructField(pm10,FloatType,true),StructField(pm25,FloatType,true)," \
     "StructField(so2,FloatType,true),StructField(t,FloatType,true)," \
     "StructField(tz,StringType,true),StructField(w,FloatType,true)))"
     )])
def test_load_and_get_table_df(keys_space_name, table_name, expected_schema):

    table_df, sc = load_table.load_and_get_table_df(keys_space_name, table_name)
    actual_schema = str(table_df.schema)
    assert actual_schema == expected_schema
    sc.stop()
