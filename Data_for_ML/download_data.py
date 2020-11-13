from cassandra.cluster import Cluster
import pandas as pd
from datetime import datetime
import time

cluster = Cluster(['127.0.0.1'], "9042")
session = cluster.connect('json')
miasto = "Paris"
cql = "select * from powietrze where name = " + str(miasto)
powietrze = session.execute(cql)
df = pd.DataFrame()
for row in powietrze:
    print(pd.DataFrame(row))


