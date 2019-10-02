import app
import os, tempfile, unittest
from db import get_db
from unittest.mock import Mock, patch

class TestCase(unittest.TestCase):

    def setUp(self):
        self.config = app.app.config
        self.client = app.app.test_client()
        self.db_fd, app.app.config['DATABASE'] = tempfile.mkstemp()
        with app.app.app_context():
            self.db = get_db()

    def tearDown(self):
        os.close(self.db_fd)
        os.unlink(app.app.config['DATABASE'])



@patch('app.query_db')
@patch('app.get_db')
class TestGetCompanyExportCountries(TestCase):
        
    def test(self, get_db, query_db):
        response =  self.client.get('get-company-export-countries')
        self.assertEquals(response.status_code, 200)

    def test_creates_a_database_connection(self, get_db, query_db):
        response = app.get_company_export_countries()
        get_db.assert_called_once_with()
        
    def test_queries_the_database(self, get_db, query_db):
        db = Mock()
        get_db.return_value = db
        response = app.get_company_export_countries()
        calls = query_db.call_args_list
        self.assertEqual(len(calls), 1)
        positional_args = calls[0][0]
        expected_db = positional_args[0]
        self.assertEqual(db, expected_db)

    def test_converts_to_a_dictionary(self, get_db, query_db):
        rows = [(0, 'germany'), (1, 'china')]
        query_db.return_value = rows
        expected = {
            'headers': ['datahubCompanyID', 'exportCountry'],
            'data': [(0, 'germany'), (1, 'china')]
        }
        response = app.get_company_export_countries()
        self.assertEqual(response, expected)


@patch('app.query_db')
@patch('app.get_db')
class TestGetCompanyCountriesOfInterest(TestCase):

    def test(self, get_db, query_db):
        response = self.client.get('get-company-countries-of-interest')
        self.assertEqual(response.status_code, 200)

    def test_creates_a_database_connection(self, get_db, query_db):
        response = app.get_company_countries_of_interest()
        get_db.assert_called_once_with()
        
    def test_queries_the_database(self, get_db, query_db):
        db = Mock()
        get_db.return_value = db
        response = app.get_company_countries_of_interest()
        calls = query_db.call_args_list
        self.assertEqual(len(calls), 1)
        positional_args = calls[0][0]
        expected_db = positional_args[0]
        self.assertEqual(db, expected_db)

    def test_converts_to_a_dictionary(self, get_db, query_db):
        rows = [(0, 'germany'), (1, 'china')]
        query_db.return_value = rows
        expected = {
            'headers': ['datahubCompanyID', 'countryOfInterest'],
            'data': [(0, 'germany'), (1, 'china')]
        }
        response = app.get_company_countries_of_interest()
        self.assertEqual(response, expected)


class TestGetCompaniesAffectedByTradeBarrier(TestCase):

    def test(self):
        response = self.client.get('get-companies-affected-by-trade-barrier/country/sector')
        self.assertEqual(response.status_code, 200)
        
