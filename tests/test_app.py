import os, tempfile, unittest
from app import app
from db import get_db

class TestCase(unittest.TestCase):

    def setUp(self):
        self.config = app.config
        self.client = app.test_client()
        self.db_fd, app.config['DATABASE'] = tempfile.mkstemp()
        with app.app_context():
            self.db = get_db()

    def tearDown(self):
        os.close(self.db_fd)
        os.unlink(app.config['DATABASE'])


class TestGetCompanyExportCountries(TestCase):
        
    def test(self):
        response =  self.client.get('get-company-export-countries')
        self.assertEquals(response.status_code, 200)


class TestGetCompanyCountriesOfInterest(TestCase):

    def test(self):
        response = self.client.get('get-company-countries-of-interest')
        self.assertEqual(response.status_code, 200)


class TestGetCompaniesAffectedByTradeBarrier(TestCase):

    def test(self):
        response = self.client.get('get-companies-affected-by-trade-barrier/country/sector')
        self.assertEqual(response.status_code, 200)
        
