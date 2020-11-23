# Poniżej ładowane są biblioteki potrzebne do poprawnej pracy interfejsu

from flask_mail import Mail, Message
from flask import Flask
from flask_login import UserMixin
from flask import render_template, redirect, url_for, request, flash
from itsdangerous import URLSafeTimedSerializer, SignatureExpired, BadSignature
from werkzeug.security import generate_password_hash, check_password_hash
from flask_login import login_user, logout_user, login_required
from cassandra.cluster import Cluster
import pandas as pd
from jinjasql import JinjaSql
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager

# Ogólne parametry wykorzystywane w poniższych endpoitach
j = JinjaSql(param_style='pyformat')
db = SQLAlchemy()
app = Flask(__name__)
app.config['SECRET_KEY'] = 'thisisimportantsomehow'
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///db.sqlite3'
app.config['MAIL_SERVER'] = 'smtp.gmail.com'
app.config['MAIL_PORT'] = 465
app.config['MAIL_USERNAME'] = 'emailakceptacyjny@gmail.com'
app.config['MAIL_PASSWORD'] = '123456780_ok'
app.config['MAIL_USE_TLS'] = False
app.config['MAIL_USE_SSL'] = True
db.init_app(app)
login_manager = LoginManager()
login_manager.login_view = 'login'
login_manager.init_app(app)
mail = Mail(app)
s = URLSafeTimedSerializer('akceptacjamailem')

# Funkcja, która wysyła link potwierdzający na podany adres email
def send_mail_confirmation(user):
    token = user.get_mail_confirm_token()
    msg = Message(
        "Zaakceptuj maila",
        sender="noreply@demo.com",
        recipients=[user.email])
    link = url_for('confirm_email', token=token, _external=True)
    msg.body = 'Your link is {}'.format(link)
    mail.send(msg)

# Endpoint odpowiedzialny za potwierdzenie adesru email
@app.route('/confirm_email/<token>')
def confirm_email(token):
    email = User.verify_mail_confirm_token(token)
    if email:
        user = db.session.query(User).filter(User.email == email).one_or_none()
        user.email_confirmed = True
        db.session.add(user)
        db.session.commit()
        flash(
            "Gratualcje, udało ci się potwierdzić mail!",
            "Zapraszamy",
        )
        return redirect(url_for("login"))
    else:
        return render_template(url_for('singup'))


# Model tabeli znajdującej się w bazie danych, przechowuje zarejstrowanych użytkowników
class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(100), unique=True)
    email_confirmed = db.Column(db.Boolean(), nullable=False, default=False)
    password = db.Column(db.String(100))
    name = db.Column(db.String(1000))

# Przekazanie tokenu, który służy do potwierdzenia adresu email
    def get_mail_confirm_token(self):
        s = URLSafeTimedSerializer(
            app.config["SECRET_KEY"], salt="email-comfirm")
        return s.dumps(self.email, salt="email-confirm")

# Mechanizm weryfikacji adresu email na podstawie tokenu
    @staticmethod
    def verify_mail_confirm_token(token):
        try:
            s = URLSafeTimedSerializer(
                app.config["SECRET_KEY"], salt="email-confirm"
            )
            email = s.loads(token, salt="email-confirm", max_age=3600)
            return email
        except (SignatureExpired, BadSignature):
            return None


# Przy pierwszym uruchomieniu tworzona jest baza i tabela
@app.before_first_request
def create_tables():
    db.create_all()


# Pierwsza strona jaka się ładuje po wejściu, formualrz w którym można przejść do logowania lub rejstracji
@app.route('/')
def index():
    return render_template('index.html')


# Odpowiada za przekierowanie do strony startowej
@app.route('/index')
def index_powrot():
    return render_template('index.html')


# Przekierowuje na formularz odpowiadający za logowanie
@app.route('/login')
def login():
    return render_template('login.html')


# Odpowaida za mechanizm logowania
@app.route('/login', methods=['POST'])
def login_post():
    # Poniżej fragment czytający dane wypełnione przez użytkownika
    email = request.form.get('email')
    password = request.form.get('password')
    remember = True if request.form.get('remember') else False
    # Sprawdzenie w bazie danych czy jest taki użytkownik
    user = User.query.filter_by(email=email).first()
    # Sprawdzenie czy taki użytkownik ma to hasło
    if not user or not check_password_hash(user.password, password):
        flash('Sprawdz wprowadzone dane.')
        return redirect(url_for('login'))
    if not user.email_confirmed:
        flash('Potwierdz konto.')
        return redirect(url_for('login'))
    login_user(user, remember=remember)

    return redirect(url_for('home'))


# Przekierowuje na formularz odpowiadający za rejstracje
@app.route('/signup')
def signup():
    return render_template('signup.html')


# Odpowaida za mechanizm logowania
@app.route('/signup', methods=['POST'])
def signup_post():
    # Poniżej fragment czytający wypełnione dane
    email = request.form.get('email')
    name = request.form.get('name')
    password = request.form.get('password')
    # Sprawdzenie czy taki użytkownik już istnieje
    user = User.query.filter_by(email=email).first()
    # Jeżeli istnieje to przesyłamy jeszcze raz na rejstracje
    if user:
        flash('Mail zajęty.')
        return redirect(url_for('signup'))
    # Tworzenie nowego wpisu do bazy danych
    new_user = User(email=email, name=name, password=generate_password_hash(password, method='sha256'))
    # Zapisanie wpisu do bazy danych
    db.session.add(new_user)
    db.session.commit()
    send_mail_confirmation(new_user)

    return redirect(url_for('login'))


# Przekierowuje na forumalrz po wylogowaniu się
@app.route('/logout')
@login_required  # To oznacza, że trzeba być zalogowanym, aby móv wykonać tę akcję, poniżej wszystkie endpointy tak mają
# Zatem nie można się do nich dostać bez wcześniejszego zalogowania
def logout():
    logout_user()
    return render_template("logout.html")


# Załądowanie użytkownika
@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))


# Strona na, którą jesteśmy przekierowani po zalogowaniu
@app.route('/home')
@login_required
def home():
    return render_template('home.html')


# Część interfejsu odpowiadająca za interakcje z danymi o powietrzu
@app.route("/powietrze")
@login_required
def powietrze():
    return render_template("powietrze.html")


# Część interfejsu odpowiadająca za interakcje z danymi o rowerach
@app.route("/velib")
@login_required
def velib():
    return render_template("velib.html")


# Część interfejsu odpowiadająca za interakcje z danymi o urzędach
@app.route("/urzedy")
@login_required
def urzedy():
    return render_template("urzedy.html")


# Pobranie z bazy danych Cassandra nazw stacji pomiaru zanieczyszczenia powietrza
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


# Pobranie z bazy danych Cassandra danych dotczących zanieczyszczenia powietrza, w podanym przez użytkownika mieście i okresie czasowym
@app.route("/powietrze/dane/<miasto>/<fromd>/<tod>", methods=['GET'])
@login_required
def get_powietrze_dane_archiwalne(miasto, fromd, tod):
    user_transaction_template = '''SELECT json * FROM powietrze where name = {{miasto}} and timestamp > {{fromd}} and timestamp < {{tod}}  allow filtering'''
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


# Pobranie z bazy danych Cassandra nazw stacji z rowerami
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


# Pobranie z bazy danych Cassandra danych dotczących zaopatrzenia stacji, w podanym przez użytkownika punkcie i okresie czasowym
@app.route("/velib/dane/<stacja>/<fromd>/<tod>", methods=['GET'])
@login_required
def get_rowery_dane_archiwalne(stacja, fromd, tod):
    user_transaction_template = '''SELECT json * FROM velib where station_id = {{stacja}} and timestamp > {{fromd}} and timestamp < {{tod}}  allow filtering'''
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


# Pobranie z bazy danych Cassandra nazw urzędów
@app.route("/urzedy/nazwy")
@login_required
def get_nazwy():
    user_transaction_template = ''' select json urzad from urzedy_nazwy '''
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


# Pobranie z bazy danych Cassandra danych o podanym przez użytkownika urzędzie
@app.route("/urzedy/<nazwa>")
@login_required
def get_okienko(nazwa):
    user_transaction_template = '''SELECT json * FROM urzedy_nazwy where urzad = {{nazwa}} allow filtering'''
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


# Pobranie z bazy danych Cassandra danych pomocniczych o urzędzie podanym przez użytkownika
@app.route("/urzedy/pomoc/<urzad>", methods=['GET'])
@login_required
def get_idgrupy(urzad):
    user_transaction_template = '''SELECT json * FROM urzedy_nazwy where urzad = {{urzad}}  allow filtering'''
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


# Pobranie z bazy danych Cassandra danych dotczących kolejek urzędowych, w podanym przez użytkownika punkcie i okresie czasowym
@app.route("/urzedy/dane/<id>/<fromd>/<tod>", methods=['GET'])
@login_required
def get_urzedy_dane_archiwalne(id, fromd, tod):
    user_transaction_template = '''SELECT json * FROM urzedy where idgrupy = {{id}} and timestamp > {{fromd}} and timestamp < {{tod}}  allow filtering'''
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


# W przypadku wywołania kodu, odpalamy aplikacje
if __name__ == '__main__':
    app.run()