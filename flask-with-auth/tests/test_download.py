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


class TestFlaskPobieranieDanych(unittest.TestCase):

    def setUp(self):
        app.config['LOGIN_DISABLED'] = True
        self.client = webtest.TestApp(app)

    def test_pob_pow_nazwy(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/powietrze/nazwy", content_type='application/json')
        self.assertEqual(odpowiedz.status_code, 200)

    def test_pob_pow_nazwy_json(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/powietrze/nazwy", content_type='application/json')
        self.assertTrue(b'nazwa' in odpowiedz.data)

    def test_pob_pow_dane(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/powietrze/dane/'1'/1/1", content_type='application/json')
        self.assertEqual(odpowiedz.status_code, 200)

    def test_pob_pow_pred(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/powietrze/predykcja/'1'", content_type='application/json')
        self.assertEqual(odpowiedz.status_code, 200)

    def test_pob_pow_stat(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/powietrze/staty/1/1", content_type='application/json')
        self.assertEqual(odpowiedz.status_code, 200)

    def test_pob_pow_statymod(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/powietrze/statymod/1/1", content_type='application/json')
        self.assertEqual(odpowiedz.status_code, 200)

    def test_pob_velib_nazwy(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/velib/stacje", content_type='application/json')
        self.assertEqual(odpowiedz.status_code, 200)

    def test_pob_velib_nazwy_json(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/velib/stacje", content_type='application/json')
        self.assertTrue(b'station_id' in odpowiedz.data)

    def test_pob_velib_dane(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/velib/dane/1/1/1", content_type='application/json')
        self.assertEqual(odpowiedz.status_code, 200)

    def test_pob_velib_pred(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/velib/predykcja/'1'", content_type='application/json')
        self.assertEqual(odpowiedz.status_code, 200)

    def test_pob_velib_stat(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/velib/staty/1/1", content_type='application/json')
        self.assertEqual(odpowiedz.status_code, 200)

    def test_pob_velib_statymod(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/velib/statymod/1/1", content_type='application/json')
        self.assertEqual(odpowiedz.status_code, 200)

    def test_pob_urzedy_nazwy(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/urzedy/nazwy", content_type='application/json')
        self.assertEqual(odpowiedz.status_code, 200)

    def test_pob_urzedy_nazwy_json(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/urzedy/nazwy", content_type='application/json')
        self.assertTrue(b'urzad' in odpowiedz.data)

    def test_pob_urzedy_okienka(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/urzedy/'1'", content_type='application/json')
        self.assertEqual(odpowiedz.status_code, 200)

    def test_pob_urzedy_pomoc_urzad(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/urzedy/pomoc/'1'", content_type='application/json')
        self.assertEqual(odpowiedz.status_code, 200)

    def test_pob_urzedy_dane(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/urzedy/dane/1/1/1", content_type='application/json')
        self.assertEqual(odpowiedz.status_code, 200)

    def test_pob_urzedy_pred(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/urzedy/predykcja/'1'", content_type='application/json')
        self.assertEqual(odpowiedz.status_code, 200)

    def test_pob_urzedy_stat(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/urzedy/staty/1/1", content_type='application/json')
        self.assertEqual(odpowiedz.status_code, 200)

    def test_pob_urzedy_statymod(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/urzedy/statymod/1/1", content_type='application/json')
        self.assertEqual(odpowiedz.status_code, 200)


if __name__ == '__main__':
    unittest.main()