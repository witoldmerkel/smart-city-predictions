from cassandra.cluster import Cluster
import pandas as pd
from datetime import datetime
import time

cluster = Cluster(['127.0.0.1'], "9042")
session = cluster.connect('json')
urzedy1 = session.execute('select * from urzedy;')
urzedy = pd.DataFrame(list(urzedy1))
tms = urzedy[['timestamp']]
data=[0]*len(tms)
inny_format=[0]*len(tms)
dzien=[0]*len(tms)
miesiac=[0]*len(tms)
rok=[0]*len(tms)
godzina=[0]*len(tms)
minuta=[0]*len(tms)
for i in range(0, len(tms)):
    inny_format[i] = datetime.fromtimestamp(tms.iloc[i,0])
    dzien[i] = inny_format[i].day
    miesiac[i] = inny_format[i].month
    rok[i] = inny_format[i].year
    godzina[i] = inny_format[i].hour
    minuta[i] = inny_format[i].minute

urzedy.insert(loc = 1, column="Dzien", value=dzien)
urzedy.insert(loc = 1, column="Miesiac", value=miesiac)
urzedy.insert(loc = 1, column="Rok", value=rok)
urzedy.insert(loc = 1, column="Godzina", value=godzina)
urzedy.insert(loc = 1, column="Minuta", value=minuta)