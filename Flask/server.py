from flask import Flask, render_template
from cassandra.cluster import Cluster
import pandas as pd

app = Flask(__name__)

@app.route("/")
def home():
    return render_template("home.html")

@app.route("/powietrze", methods=['GET'])
def get_powietrze():
    cluster = Cluster(['127.0.0.1'], "9042")
    session = cluster.connect('json')
    cql = "SELECT * FROM powietrze"
    r = session.execute(cql)
    return str(r[0])

@app.route("/urzedy", methods=['GET'])
def get_urzedy():
    cluster = Cluster(['127.0.0.1'], "9042")
    session = cluster.connect('json')
    cql = "SELECT * FROM urzedy"
    r = session.execute(cql)
    return str(r[0])

@app.route("/velib", methods=['GET'])
def get_velib():
    cluster = Cluster(['127.0.0.1'], "9042")
    session = cluster.connect('json')
    cql = "SELECT * FROM velib"
    r = session.execute(cql)
    return str(r[0])

if __name__ == '__main__':
    app.run()