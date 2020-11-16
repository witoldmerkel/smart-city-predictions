from flask import Flask, render_template
from cassandra.cluster import Cluster
import pandas as pd

app = Flask(__name__)

@app.route("/")
def home():
    return render_template("home.html")

@app.route("/powietrze")
def powietrze():
    return render_template("powietrze.html")

@app.route("/velib")
def velib():
    return render_template("velib.html")

@app.route("/urzedy")
def urzedy():
    return render_template("urzedy.html")

@app.route("/powietrze/nazwy")
def get_nazwy_punktow():
    cluster = Cluster(['127.0.0.1'], "9042")
    session = cluster.connect('json')
    cql = "SELECT name from powietrze where timestamp = 1605553200 allow filtering"
    r = session.execute(cql)
    df = pd.DataFrame()
    for row in r:
        df = df.append(pd.DataFrame(row))
    return str(df.to_json(orient="records"))

@app.route("/powietrze/dane/<miasto>/<fromd>/<tod>", methods=['GET'])
def get_powietrze_dane_archiwalne(miasto, fromd, tod):
    cluster = Cluster(['127.0.0.1'], "9042")
    session = cluster.connect('json')
    cql = "SELECT json * FROM powietrze where name =" + miasto + " and timestamp >" + fromd + " and timestamp <" + tod
    r = session.execute(cql)
    df = pd.DataFrame()
    for row in r:
        df = df.append(pd.DataFrame(row))
    return str(df.to_json(orient="records"))

@app.route("/velib/stacje")
def get_stacje():
    cluster = Cluster(['127.0.0.1'], "9042")
    session = cluster.connect('json')
    cql = "SELECT json * from velib_stations"
    r = session.execute(cql)
    df = pd.DataFrame()
    for row in r:
        df = df.append(pd.DataFrame(row))
    return str(df.to_json(orient="records"))

@app.route("/velib/dane/<stacja>/<fromd>/<tod>", methods=['GET'])
def get_rowery_dane_archiwalne(stacja, fromd, tod):
    cluster = Cluster(['127.0.0.1'], "9042")
    session = cluster.connect('json')
    cql = "SELECT json * FROM velib where station_id =" + stacja + " and timestamp >" + fromd + " and timestamp <" + tod
    r = session.execute(cql)
    df = pd.DataFrame()
    for row in r:
        df = df.append(pd.DataFrame(row))
    return str(df.to_json(orient="records"))

@app.route("/urzedy/nazwy")
def get_nazwy():
    cluster = Cluster(['127.0.0.1'], "9042")
    session = cluster.connect('json')
    cql = "SELECT json * from urzedy_nazwy"
    r = session.execute(cql)
    df = pd.DataFrame()
    for row in r:
        df = df.append(pd.DataFrame(row))
    return str(df.to_json(orient="records"))

@app.route("/urzedy/dane/<urzad>/<fromd>/<tod>", methods=['GET'])
def get_urzedy_dane_archiwalne(urzad, fromd, tod):
    cluster = Cluster(['127.0.0.1'], "9042")
    session = cluster.connect('json')
    cql = "SELECT json * FROM urzedy where idgrupy =" + urzad + " and timestamp >" + fromd + " and timestamp <" + tod
    r = session.execute(cql)
    df = pd.DataFrame()
    for row in r:
        df = df.append(pd.DataFrame(row))
    return str(df.to_json(orient="records"))

if __name__ == '__main__':
    app.run()