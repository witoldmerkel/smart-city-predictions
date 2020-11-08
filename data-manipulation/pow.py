from cassandra.cluster import Cluster
import pandas as pd
from datetime import datetime
import time

cluster = Cluster(['127.0.0.1'], "9042")
session = cluster.connect('json')
powietrze1 = session.execute('select * from powietrze;')
powietrze = pd.DataFrame(list(powietrze1))
tms = powietrze[['timestamp']]
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

powietrze.insert(loc = 1, column="Dzien", value=dzien)
powietrze.insert(loc = 1, column="Miesiac", value=miesiac)
powietrze.insert(loc = 1, column="Rok", value=rok)
powietrze.insert(loc = 1, column="Godzina", value=godzina)
powietrze.insert(loc = 1, column="Minuta", value=minuta)

target = powietrze[['pm25']]
