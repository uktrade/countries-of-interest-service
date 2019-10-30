import app
import datetime, psycopg2, os, tempfile, unittest
from db import get_db
from unittest.mock import Mock, patch
from tests.TestCase import TestCase
from etl import countries_and_sectors_of_interest


class TestCaseHawkAuthenticated(TestCase):

    @patch('app.query_database')
    @patch('app.to_web_dict')
    @patch('authentication.hawk_authenticate')
    def test_hawk_authentication(self, hawk_authenticate, to_web_dict, query_database):
        to_web_dict.return_value = {'headers': ['header1', 'header2'], 'values': [[1, 2], [3, 4]]}
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


@patch('authentication.hawk_authenticate')
class TestGetCompaniesAffectedByTradeBarrier(TestCase):

    def test(self, hawk_authenticate):
        schema = countries_and_sectors_of_interest.table_fields
        table_name = countries_and_sectors_of_interest.table_name
        url = '/api/get-companies-affected-by-trade-barrier/CN/Aerospace'
        values = [
            ('asdf', 'CN', 'Aerospace', 'omis', '123', '2019-01-01'),
            ('asdf33', 'US', 'Food', 'omis', '345', '2019-01-02'),
        ]
        with app.app.app_context():
            connection = get_db()
            connection.autocommit = True
            cursor = connection.cursor()
        sql = ''' create table {} {} '''.format(table_name, schema)
        cursor.execute(sql)
        sql = ''' insert into {} values (%s, %s, %s, %s, %s, %s) '''.format(table_name)
        cursor.executemany(sql, values)
        response = self.client.get(url)
        expected = {
            'headers': ['companiesHouseCompanyNumber'],
            'values': ['asdf']
        }
        self.assertEqual(response.json, expected)
