# Importing modules
import pandas as pd
from cassandra.cluster import Cluster


# Makes loading faster
def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)


# Connecting to data base in cassandra
cluster = Cluster(['127.0.0.1'], "9042")
session = cluster.connect('json')
session.row_factory = pandas_factory
session.default_fetch_size = None


# Polecenia CQL czytające dane z bazy danych
query1 = "SELECT avg(temp), timezone from weather group by timezone"
result1 = session.execute(query1, timeout=None)



df1 = result1._current_rows

print(df1)
