import pandas as pd

from cassandra.cluster import Cluster

cluster = Cluster(contact_points=["127.0.0.1"], port="9042")
session = cluster.connect("json")

#urzedy = session.execute('select * from urzedy;')
#powietrze = session.execute('select * from powietrze;')
#velib = session.execute('select * from velib;')
#velib_stacje = session.execute('select * from velib_stations;')
#urzedy_nazwy = session.execute('select * from urzedy_nazwy;')

def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)


session.row_factory = pandas_factory
session.default_fetch_size = None


query = "SELECT * FROM {}.{};".format("json", "urzedy")

df = pd.DataFrame()

rslt = session.execute(query, timeout=None)
df = rslt._current_rows

print(df.shape)