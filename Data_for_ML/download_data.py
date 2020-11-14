from cassandra.cluster import Cluster
import pandas as pd
from datetime import datetime
import time
start_time = time.time()
cluster = Cluster(['127.0.0.1'], "9042")
session = cluster.connect('json')
cql = "select * from velib where station_id = 102328355"
velib = session.execute(cql)
df = pd.DataFrame()
for row in velib:
    df = df.append(pd.DataFrame(row))

print("--- %s seconds ---" % (time.time() - start_time))

