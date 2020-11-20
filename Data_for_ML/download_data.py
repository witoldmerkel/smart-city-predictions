from flask import Flask, render_template
from cassandra.cluster import Cluster
import pandas as pd
from jinjasql import JinjaSql

j = JinjaSql(param_style='pyformat')
user_transaction_template = ''' select json urzad from urzedy_nazwy'''
params = {}
query, bind_params = j.prepare_query(user_transaction_template, params)
cql = query % bind_params
cluster = Cluster(['127.0.0.1'], "9042")
session = cluster.connect('json')
r = session.execute(cql)
df = pd.DataFrame()
for row in r:
    df = df.append(pd.DataFrame(row))

df.columns = ["urzad"]
df = df.urzad.unique()
print(type(df))
df = pd.DataFrame(df)
print(type(df))
