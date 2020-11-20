from jinjasql import JinjaSql
import pandas as pd
from six import string_types
from copy import deepcopy
from cassandra.cluster import Cluster
cluster = Cluster(['127.0.0.1'], "9042")
session = cluster.connect('json')
liczba = 1
params = {
    'id': liczba,
}
user_transaction_template = ''' select json * from powietrze_nazwy where id = {{ id }} allow filtering'''

j = JinjaSql(param_style='pyformat')
query, bind_params = j.prepare_query(user_transaction_template, params)


def quote_sql_string(value):
    '''
    If `value` is a string type, escapes single quotes in the string
    and returns the string enclosed in single quotes.
    '''
    if isinstance(value, string_types):
        new_value = str(value)
        new_value = new_value.replace("'", "''")
        return "'{}'".format(new_value)
    return value


def get_sql_from_template(query, bind_params):
    if not bind_params:
        return query
    params = deepcopy(bind_params)
    for key, val in params.items():
        params[key] = quote_sql_string(val)
    return query % params

print(get_sql_from_template(query, bind_params))
df = pd.DataFrame()

cluster = Cluster(['127.0.0.1'], "9042")
session = cluster.connect('json')
cql = get_sql_from_template(query, bind_params)
r = session.execute(cql)
df = pd.DataFrame()
for row in r:
    df = df.append(pd.DataFrame(row))

print(df)
#for row in r:
 #   df = df.append(pd.DataFrame(row))