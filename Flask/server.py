from flask import Flask, render_template, jsonify, Response
from cassandra.cluster import Cluster
import pandas as pd
from flask_wtf import FlaskForm

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

@app.route("/powietrze/dane/<miasto>/<fromd>/<tod>", methods=['GET'])
def get_powietrze(miasto, fromd, tod):
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
def get_rowery(stacja, fromd, tod):
    cluster = Cluster(['127.0.0.1'], "9042")
    session = cluster.connect('json')
    cql = "SELECT json * FROM velib where station_id =" + stacja + " and timestamp >" + fromd + " and timestamp <" + tod
    r = session.execute(cql)
    df = pd.DataFrame()
    for row in r:
        df = df.append(pd.DataFrame(row))
    return str(df.to_json(orient="records"))

if __name__ == '__main__':
    app.run()