import os
import tempfile
import sqlalchemy
import webtest

from main import app
import unittest


class TestFlaskaLadowanieStronZablokowanych(unittest.TestCase):

    def setUp(self):
        app.config['LOGIN_DISABLED'] = True
        self.client = webtest.TestApp(app)

    def test_logout(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/logout", content_type='html/text')
        self.assertEqual(odpowiedz.status_code, 200)

    def test_logout_zawartosc(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/logout", content_type='html/text')
        self.assertTrue(b'za skorzystanie! Do zobaczenia!' in odpowiedz.data)

    def test_powietrze(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/powietrze", content_type='html/text')
        self.assertEqual(odpowiedz.status_code, 200)

    def test_powietrze_zawartosc(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/powietrze", content_type='html/text')
        self.assertTrue(b'w sekcji: powietrze' in odpowiedz.data)

    def test_home(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/home", content_type='html/text')
        self.assertEqual(odpowiedz.status_code, 200)

    def test_home_zawartosc(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/home", content_type='html/text')
        self.assertTrue(b'Witamy w stronie' in odpowiedz.data)

    def test_urzedy(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/urzedy", content_type='html/text')
        self.assertEqual(odpowiedz.status_code, 200)

    def test_urzedy_zawartosc(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/urzedy", content_type='html/text')
        self.assertTrue(b'w sekcji: urz' in odpowiedz.data)

    def test_velib(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/velib", content_type='html/text')
        self.assertEqual(odpowiedz.status_code, 200)

    def test_velib_zawartosc(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/velib", content_type='html/text')
        self.assertTrue(b'w sekcji: rowery' in odpowiedz.data)
