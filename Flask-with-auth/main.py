from flask import Flask
from flask_login import UserMixin
from flask import Blueprint, render_template, redirect, url_for, request, flash
from werkzeug.security import generate_password_hash, check_password_hash
from flask_login import login_user, logout_user, login_required
from cassandra.cluster import Cluster
import pandas as pd
from jinjasql import JinjaSql
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager

j = JinjaSql(param_style='pyformat')
db = SQLAlchemy()
app = Flask(__name__)
app.config['SECRET_KEY'] = 'thisisimportantsomehow'
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///db.sqlite3'
db.init_app(app)
login_manager = LoginManager()
login_manager.login_view = 'login'
login_manager.init_app(app)


class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(100), unique=True)
    password = db.Column(db.String(100))
    name = db.Column(db.String(1000))


@app.before_first_request
def create_tables():
    db.create_all()


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/index')
def index_powrot():
    return render_template('index.html')


@app.route('/login')
def login():
    return render_template('login.html')


@app.route('/login', methods=['POST'])
def login_post():
    email = request.form.get('email')
    password = request.form.get('password')
    remember = True if request.form.get('remember') else False

    user = User.query.filter_by(email=email).first()

    if not user and not check_password_hash(user.password, password):
        flash('Please check your login details and try again.')
        return redirect(url_for('login'))

    login_user(user, remember=remember)

    return redirect(url_for('home'))


@app.route('/signup')
def signup():
    return render_template('signup.html')


@app.route('/signup', methods=['POST'])
def signup_post():
    email = request.form.get('email')
    name = request.form.get('name')
    password = request.form.get('password')

    user = User.query.filter_by(email=email).first()

    if user:
        flash('Email address already exists.')
        return redirect(url_for('signup'))

    new_user = User(email=email, name=name, password=generate_password_hash(password, method='sha256'))

    db.session.add(new_user)
    db.session.commit()

    return redirect(url_for('login'))


@app.route('/logout')
@login_required
def logout():
    logout_user()
    return render_template("logout.html")


@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))


@app.route('/home')
@login_required
def home():
    return render_template('home.html')


@app.route("/powietrze")
@login_required
def powietrze():
    return render_template("powietrze.html")


@app.route("/velib")
@login_required
def velib():
    return render_template("velib.html")


@app.route("/urzedy")
@login_required
def urzedy():
    return render_template("urzedy.html")

@app.route("/powietrze/nazwy")
@login_required
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
@login_required
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
@login_required
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
@login_required
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
@login_required
def get_nazwy():
    user_transaction_template = ''' select json urzad from urzedy_nazwy'''
    params = {}
    query, bind_params = j.prepare_query(user_transaction_template, params)
    cql = query % bind_params
    cluster = Cluster(['127.0.0.1'], "9042")
    session = cluster.connect('json')
    r = session.execute(cql)
    df = pd.DataFrame()
    for row in r:
        df = df.append(pd.DataFrame(row))
    df.columns = ["urzad"]
    df = df.urzad.unique()
    df = pd.DataFrame(df)
    return str(df.to_json(orient="records"))

@app.route("/urzedy/<nazwa>")
@login_required
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
@login_required
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
@login_required
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