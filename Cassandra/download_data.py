import pandas as pd

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import dict_factory


cluster = Cluster(contact_points=["127.0.0.1"], port="9042")
session = cluster.connect("json")


def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)


session.row_factory = pandas_factory
session.default_fetch_size = None


query = "SELECT * FROM {}.{};".format("json", "velib")

df = pd.DataFrame()

rslt = session.execute(query, timeout=None)
df = rslt._current_rows

print(df.shape)