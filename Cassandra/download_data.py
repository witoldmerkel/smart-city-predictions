import pandas as pd

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import dict_factory


cluster = Cluster(contact_points=["127.0.0.1"], port="9042")

session = cluster.connect("json")
session.row_factory = dict_factory

sql_query = "SELECT * FROM {}.{};".format("json", "urzedy_nazwy")

df = pd.DataFrame()

for row in session.execute(sql_query):
    df = df.append(pd.DataFrame(row, index=[0]))

df = df.reset_index(drop=True)

print(df.shape)