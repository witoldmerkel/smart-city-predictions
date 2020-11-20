from flask import Flask, render_template
from cassandra.cluster import Cluster
import pandas as pd
from jinjasql import JinjaSql


j = JinjaSql(param_style='pyformat')

app = Flask(__name__)
app.config['TEMPLATES_AUTO_RELOAD'] = True
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0

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
    user_transaction_template = ''' select json * from powietrze_nazwy'''
    params = {}
    query, bind_params = j.prepare_query(user_transaction_template, params)
    cql = query % bind_params
    cluster = Cluster(['127.0.0.1'], "9042")
    session = cluster.connect('json')
    r = session.execute(cql)
    df = pd.DataFrame()
    for row in r:
        df = df.append(pd.DataFrame(row))
    return str(df.to_json(orient="records"))


@app.route("/powietrze/dane/<miasto>/<fromd>/<tod>", methods=['GET'])
def get_powietrze_dane_archiwalne(miasto, fromd, tod):
    user_transaction_template = '''SELECT json * FROM powietrze where name = {{miasto}} and timestamp > {{fromd}} and timestamp < {{tod}}'''
    params = {
        'miasto': miasto,
        'fromd': fromd,
        'tod': tod,
    }
    query, bind_params = j.prepare_query(user_transaction_template, params)
    cql = query % bind_params
    cluster = Cluster(['127.0.0.1'], "9042")
    session = cluster.connect('json')
    r = session.execute(cql)
    df = pd.DataFrame()
    for row in r:
        df = df.append(pd.DataFrame(row))
    return str(df.to_json(orient="records"))


@app.route("/velib/stacje")
def get_stacje():
    user_transaction_template = ''' select json * from velib_stations'''
    params = {}
    query, bind_params = j.prepare_query(user_transaction_template, params)
    cql = query % bind_params
    cluster = Cluster(['127.0.0.1'], "9042")
    session = cluster.connect('json')
    r = session.execute(cql)
    df = pd.DataFrame()
    for row in r:
        df = df.append(pd.DataFrame(row))
    return str(df.to_json(orient="records"))


@app.route("/velib/dane/<stacja>/<fromd>/<tod>", methods=['GET'])
def get_rowery_dane_archiwalne(stacja, fromd, tod):
    user_transaction_template = '''SELECT json * FROM velib where station_id = {{stacja}} and timestamp > {{fromd}} and timestamp < {{tod}}'''
    params = {
        'stacja': stacja,
        'fromd': fromd,
        'tod': tod,
    }
    query, bind_params = j.prepare_query(user_transaction_template, params)
    cql = query % bind_params
    cluster = Cluster(['127.0.0.1'], "9042")
    session = cluster.connect('json')
    r = session.execute(cql)
    df = pd.DataFrame()
    for row in r:
        df = df.append(pd.DataFrame(row))
    return str(df.to_json(orient="records"))


@app.route("/urzedy/nazwy")
def get_nazwy():
    user_transaction_template = ''' select json * from urzedy_nazwy'''
    params = {}
    query, bind_params = j.prepare_query(user_transaction_template, params)
    cql = query % bind_params
    cluster = Cluster(['127.0.0.1'], "9042")
    session = cluster.connect('json')
    r = session.execute(cql)
    df = pd.DataFrame()
    for row in r:
        df = df.append(pd.DataFrame(row))
    return str(df.to_json(orient="records"))

@app.route("/urzedy/<nazwa>")
def get_okienko(nazwa):
    user_transaction_template = '''SELECT json * FROM urzedy_nazwy where urzad = {{nazwa}}'''
    params = {
        'nazwa': nazwa,
    }
    query, bind_params = j.prepare_query(user_transaction_template, params)
    cql = query % bind_params
    cluster = Cluster(['127.0.0.1'], "9042")
    session = cluster.connect('json')
    r = session.execute(cql)
    df = pd.DataFrame()
    for row in r:
        df = df.append(pd.DataFrame(row))
    return str(df.to_json(orient="records"))


@app.route("/urzedy/pomoc/<urzad>", methods=['GET'])
def get_idgrupy(urzad):
    user_transaction_template = '''SELECT json * FROM urzedy_nazwy where urzad = {{urzad}}'''
    params = {
        'urzad': urzad,
    }
    query, bind_params = j.prepare_query(user_transaction_template, params)
    cql = query % bind_params
    cluster = Cluster(['127.0.0.1'], "9042")
    session = cluster.connect('json')
    r = session.execute(cql)
    df = pd.DataFrame()
    for row in r:
        df = df.append(pd.DataFrame(row))
    return str(df.to_json(orient="records"))


@app.route("/urzedy/dane/<id>/<fromd>/<tod>", methods=['GET'])
def get_urzedy_dane_archiwalne(id, fromd, tod):
    user_transaction_template = '''SELECT json * FROM urzedy where idgrupy = {{id}} and timestamp > {{fromd}} and timestamp < {{tod}}'''
    params = {
        'id': id,
        'fromd': fromd,
        'tod': tod,
    }
    query, bind_params = j.prepare_query(user_transaction_template, params)
    cql = query % bind_params
    cluster = Cluster(['127.0.0.1'], "9042")
    session = cluster.connect('json')
    r = session.execute(cql)
    df = pd.DataFrame()
    for row in r:
        df = df.append(pd.DataFrame(row))
    return str(df.to_json(orient="records"))


if __name__ == '__main__':
    app.run()