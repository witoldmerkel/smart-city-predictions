from main import app
import unittest

class TestFlaskaPodstaw(unittest.TestCase):

    # Sprawdzamy czy aplikacja poprawnie się odpaliła
    def test_index(self):
        tester = app.test_client(self)
        odpowiedz = tester.get("/", content_type = 'html/text')
        self.assertEqual(odpowiedz.status_code, 200)


if __name__ == '__main__':
    unittest.main()