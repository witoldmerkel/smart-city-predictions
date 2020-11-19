from cassandra.cluster import Cluster
import pandas as pd
from datetime import datetime
import time
cluster = Cluster(['127.0.0.1'], "9042")
session = cluster.connect('json')
nazwa = "'UD Urus'"
cql = "SELECT json * from urzedy_nazwy where urzad =" + nazwa
r = session.execute(cql)
df = pd.DataFrame()
for row in r:
    df = df.append(pd.DataFrame(row))
print(df)