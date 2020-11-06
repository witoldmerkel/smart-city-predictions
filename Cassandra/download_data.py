from cassandra.cluster import Cluster
cluster = Cluster(['127.0.0.1'], "9042")  # provide contact points and port
session = cluster.connect('json')
urzedy = session.execute('select * from urzedy;')
powietrze = session.execute('select * from powietrze;')
velib = session.execute('select * from velib;')
velib_stacje = session.execute('select * from velib_stations;')
urzedy_nazwy = session.execute('select * from urzedy_nazwy;')