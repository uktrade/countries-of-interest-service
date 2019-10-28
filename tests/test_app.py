import app
import datetime, psycopg2, os, tempfile, unittest
from db import get_db
from unittest.mock import Mock, patch


class TestCase(unittest.TestCase):

    def setUp(self):
        self.config = app.app.config
        self.client = app.app.test_client()
        connection = psycopg2.connect('postgresql://postgres@localhost')
        connection.autocommit = True
        cursor = connection.cursor()
        try:
            cursor.execute(''' create database test_countries_of_interest_service; ''')
            cursor.execute(''' create user test_countries_of_interest_service; ''')
            cursor.execute(''' grant all privileges on database ''' \
                           ''' test_countries_of_interest_service ''' \
                           ''' to test_countries_of_interest_service; '''
            )
        except:
            cursor.execute(''' drop database test_countries_of_interest_service; ''')
            cursor.execute(''' drop user test_countries_of_interest_service; ''')
            
        with app.app.app_context():
            db = get_db()

    def tearDown(self):
        connection = psycopg2.connect('postgresql://postgres@localhost')
        connection.autocommit = True
        cursor = connection.cursor()
        cursor.execute(''' drop database test_countries_of_interest_service; ''')
        cursor.execute( ''' drop user test_countries_of_interest_service; ''')


class TestCaseHawkAuthenticated(TestCase):

    @patch('authentication.hawk_authenticate')
    def test_hawk_authentication(self, hawk_authenticate, *args, **kwargs):
        hawk_authenticate.side_effect = Exception('asdf')
        urls = [
            '/api/get-companies-affected-by-trade-barrier/country/sector',
            '/api/get-companies-house-company-numbers',
            '/api/get-company-countries-and-sectors-of-interest',
            '/api/get-company-countries-of-interest',
            '/api/get-company-export-countries',
            '/api/get-company-sectors-of-interest',
            '/api/get-datahub-company-ids',
            '/api/get-datahub-company-ids-to-companies-house-company-numbers',
            '/api/get-sectors',
        ]
        for url in urls:
            response =  self.client.get(url)
            self.assertEqual(response.status_code, 401)

        
@patch('app.query_db')
@patch('app.get_db')
@patch('authentication.hawk_authenticate')
class TestGetCompanyExportCountries(TestCase):

    def __init__(self, *args, **kwargs):
        self.example_url = '/api/get-company-export-countries'
        super().__init__(*args, **kwargs)

    def test(self, hawk_authenticate, get_db, query_db):
        response =  self.client.get('/api/get-company-export-countries')
        self.assertEqual(response.status_code, 200)

    def test_creates_a_database_connection(self, hawk_authenticate, get_db, query_db):
        response = app.get_company_export_countries()
        get_db.assert_called_once_with()

    def test_queries_the_database(self, hawk_authenticate, get_db, query_db):
        db = Mock()
        get_db.return_value = db
        response = app.get_company_export_countries()
        calls = query_db.call_args_list
        self.assertEqual(len(calls), 1)
        positional_args = calls[0][0]
        expected_db = positional_args[0]
        self.assertEqual(db, expected_db)

    def test_converts_to_a_dictionary(self, hawk_authenticate, get_db, query_db):
        rows = [(0, 'germany'), (1, 'china')]
        query_db.return_value = rows
        expected = {
            'headers': [
                'companiesHouseCompanyNumber',
                'exportCountry',
                'source',
                'sourceId',
                'timestamp'
            ],
            'values': [(0, 'germany'), (1, 'china')]
        }
        response = app.get_company_export_countries()
        self.assertEqual(response, expected)
        

@patch('app.query_db')
@patch('app.get_db')
@patch('authentication.hawk_authenticate')
class TestGetCompanyCountriesOfInterest(TestCase):

    def test(self, hawk_authenticate, get_db, query_db):
        response = self.client.get('/api/get-company-countries-of-interest')
        self.assertEqual(response.status_code, 200)

    def test_creates_a_database_connection(self, hawk_authenticate, get_db, query_db):
        response = app.get_company_countries_of_interest()
        get_db.assert_called_once_with()
        
    def test_queries_the_database(self, hawk_authenticate, get_db, query_db):
        db = Mock()
        get_db.return_value = db
        response = app.get_company_countries_of_interest()
        calls = query_db.call_args_list
        self.assertEqual(len(calls), 1)
        positional_args = calls[0][0]
        expected_db = positional_args[0]
        self.assertEqual(db, expected_db)

    def test_converts_to_a_dictionary(self, hawk_authenticate, get_db, query_db):
        rows = [
            (0, 'germany', 'datahub_order', '123', datetime.datetime(2019, 1, 1)),
            (1, 'china', 'datahub_order', '444', datetime.datetime(2019, 2, 1))
        ]
        query_db.return_value = rows
        expected = {
            'headers': [
                'companiesHouseCompanyNumber',
                'countryOfInterest',
                'source',
                'sourceId',
                'timestamp'
            ],
            'values': [
                (0, 'germany', 'datahub_order', '123', datetime.datetime(2019, 1, 1)),
                (1, 'china', 'datahub_order', '444', datetime.datetime(2019, 2, 1))
            ]
        }
        response = app.get_company_countries_of_interest()
        self.assertEqual(response, expected)

    def test_unauthenticated_hawk_request(self, hawk_authenticate, get_db, query_db):
        hawk_authenticate.side_effect = Exception('asdf')
        response =  self.client.get('/api/get-company-countries-of-interest')
        self.assertEqual(response.status_code, 401)

        
@patch('app.query_db')
@patch('app.get_db')
@patch('authentication.hawk_authenticate')
class TestGetCompaniesAffectedByTradeBarrier(TestCase):

    def test(self, hawk_authenticate, get_db, query_db):
        response = self.client.get('/api/get-companies-affected-by-trade-barrier/country/sector')
        self.assertEqual(response.status_code, 200)

    def test_creates_a_database_connection(self, hawk_authenticate, get_db, query_db):
        response = app.get_companies_affected_by_trade_barrier('country', 'sector')
        get_db.assert_called_once_with()
        
    def test_queries_the_database(self, hawk_authenticate, get_db, query_db):
        db = Mock()
        get_db.return_value = db
        response = app.get_companies_affected_by_trade_barrier('country', 'sector')
        calls = query_db.call_args_list
        self.assertEqual(len(calls), 1)
        positional_args = calls[0][0]
        expected_db = positional_args[0]
        self.assertEqual(db, expected_db)

    def test_converts_to_a_dictionary(self, hawk_authenticate, get_db, query_db):
        rows = [(0,), (1,)]
        query_db.return_value = rows
        expected = {
            'headers': ['companiesHouseCompanyNumber'],
            'values': [0, 1]
        }
        response = app.get_companies_affected_by_trade_barrier('country', 'sector')
        self.assertEqual(response, expected)


