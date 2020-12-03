import load_table
import pytest


@pytest.mark.parametrize('keys_space_name, table_name', [('json', 'powietrze')])
def test_load_and_get_table_df(keys_space_name, table_name):

    table_df, spark = load_table.load_and_get_table_df(keys_space_name, table_name)
    expected_schema = "StructType(List(StructField(name,StringType,false),StructField(timestamp,LongType,true)," \
                      "StructField(co,FloatType,true),StructField(h,FloatType,true),StructField(lat,FloatType,true)," \
                      "StructField(long,FloatType,true),StructField(no2,FloatType,true)," \
                      "StructField(o3,FloatType,true),StructField(p,FloatType,true)," \
                      "StructField(pm10,FloatType,true),StructField(pm25,FloatType,true)," \
                      "StructField(so2,FloatType,true),StructField(t,FloatType,true)," \
                      "StructField(tz,StringType,true),StructField(w,FloatType,true)))"
    actual_schema = str(table_df.schema)
    assert actual_schema == expected_schema
    spark.stop()
