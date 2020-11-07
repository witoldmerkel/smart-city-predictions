from cassandra.cluster import Cluster
import pandas as pd
from datetime import datetime
import time

cluster = Cluster(['127.0.0.1'], "9042")
session = cluster.connect('json')
powietrze = session.execute('select * from powietrze;')
urzedy = session.execute('select * from urzedy;')
velib = session.execute('select * from velib;')
urzedy_nazwy = session.execute('select * from urzedy_nazwy;')
velib_stacje = session.execute('select * from velib_stations;')

