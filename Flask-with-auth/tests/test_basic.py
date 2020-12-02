from main import app
import unittest


def signup(self, email, haslo):
    return self.app.post(
        '/register',
        data=dict(email=email, haslo=haslo),
        follow_redirects=True)


def login(self, email, haslo):
    return self.app.post(
        '/login',
        data=dict(email=email, haslo=haslo),
        follow_redirects=True)


def logout(self):
    return self.app.get(
        '/logout',
        follow_redirects=True)

class TestFlaskaPodstaw(unittest.TestCase):

    # Sprawdzamy czy aplikacja poprawnie się odpaliła
    def test_index(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/", content_type = 'html/text')
        self.assertEqual(odpowiedz.status_code, 200)

    # Sprawdzamy czy strona startowa się poprawnie załadowała
    def test_index_cont(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/", content_type='html/text')
        self.assertTrue(b'Witamy!' in odpowiedz.data)

    # Sprawdzamy czy poprawnie odpaliła się storna logowania
    def test_login(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/login", content_type = 'html/text')
        self.assertEqual(odpowiedz.status_code, 200)

    # Sprawdzamy czy strona logowania się poprawnie załadowała
    def test_login_cont(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/login", content_type='html/text')
        self.assertTrue(b'Zaloguj' in odpowiedz.data)

    # Sprawdzamy czy poprawnie odpaliła się storna rejstracji
    def test_signup(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/signup", content_type = 'html/text')
        self.assertEqual(odpowiedz.status_code, 200)

    # Sprawdzamy czy strona logowania się poprawnie załadowała
    def test_signup_cont(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/signup", content_type='html/text')
        self.assertTrue(b'Zarejstruj' in odpowiedz.data)

    # Poprawne logowanie

    # Niepoprawne logowanie

    # Poprawna rejstracja

    # Niepoprawna rejstracja


class TestFlaskPrzekierowywanie(unittest.TestCase):
    # Tutaj sprawdzimy czy dla kazdego endpointa chronionego haslem nastepuje przekierowanie

    def test_przek_home(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/home", content_type='html/text')
        self.assertEqual(odpowiedz.status_code, 302)

    def test_przek_logout(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/logout", content_type='html/text')
        self.assertEqual(odpowiedz.status_code, 302)

    def test_przek_pow(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/powietrze", content_type='html/text')
        self.assertEqual(odpowiedz.status_code, 302)

    def test_przek_urzedy(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/urzedy", content_type='html/text')
        self.assertEqual(odpowiedz.status_code, 302)

    def test_przek_velib(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/velib", content_type='html/text')
        self.assertEqual(odpowiedz.status_code, 302)

    def test_przek_pow_nazwy(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/powietrze/nazwy", content_type='html/text')
        self.assertEqual(odpowiedz.status_code, 302)

    def test_przek_pow_dane(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/powietrze/dane/1/1/1", content_type='html/text')
        self.assertEqual(odpowiedz.status_code, 302)

    def test_przek_pow_pred(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/powietrze/predykcja/1", content_type='html/text')
        self.assertEqual(odpowiedz.status_code, 302)

    def test_przek_pow_stat(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/powietrze/staty/1/1", content_type='html/text')
        self.assertEqual(odpowiedz.status_code, 302)

    def test_przek_pow_statymod(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/powietrze/statymod/1/1", content_type='html/text')
        self.assertEqual(odpowiedz.status_code, 302)

    def test_przek_velib_nazwy(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/velib/stacje", content_type='html/text')
        self.assertEqual(odpowiedz.status_code, 302)

    def test_przek_velib_dane(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/velib/dane/1/1/1", content_type='html/text')
        self.assertEqual(odpowiedz.status_code, 302)

    def test_przek_velib_pred(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/velib/predykcja/1", content_type='html/text')
        self.assertEqual(odpowiedz.status_code, 302)

    def test_przek_velib_stat(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/velib/staty/1/1", content_type='html/text')
        self.assertEqual(odpowiedz.status_code, 302)

    def test_przek_velib_statymod(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/velib/statymod/1/1", content_type='html/text')
        self.assertEqual(odpowiedz.status_code, 302)

    def test_przek_urzedy_nazwy(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/urzedy/nazwy", content_type='html/text')
        self.assertEqual(odpowiedz.status_code, 302)

    def test_przek_urzedy_okienka(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/urzedy/1", content_type='html/text')
        self.assertEqual(odpowiedz.status_code, 302)

    def test_przek_urzedy_pomoc_urzad(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/urzedy/pomoc/1", content_type='html/text')
        self.assertEqual(odpowiedz.status_code, 302)

    def test_przek_urzedy_dane(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/urzedy/dane/1/1/1", content_type='html/text')
        self.assertEqual(odpowiedz.status_code, 302)

    def test_przek_urzedy_pred(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/urzedy/predykcja/1", content_type='html/text')
        self.assertEqual(odpowiedz.status_code, 302)

    def test_przek_urzedy_stat(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/urzedy/staty/1/1", content_type='html/text')
        self.assertEqual(odpowiedz.status_code, 302)

    def test_przek_urzedy_statymod(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/urzedy/statymod/1/1", content_type='html/text')
        self.assertEqual(odpowiedz.status_code, 302)

if __name__ == '__main__':
    unittest.main()