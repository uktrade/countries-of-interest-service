import app
import datetime, psycopg2, os, tempfile, unittest
from db import get_db
from unittest.mock import Mock, patch
from tests.TestCase import TestCase
from etl import (
    countries_and_sectors_of_interest,
    countries_of_interest,
    export_countries,
)


class TestCaseHawkAuthenticated(TestCase):

    @patch('app.query_database')
    @patch('app.to_web_dict')
    @patch('authentication.hawk_authenticate')
    def test_hawk_authentication(self, hawk_authenticate, to_web_dict, query_database):
        to_web_dict.return_value = {'headers': ['header1', 'header2'], 'values': [[1, 2], [3, 4]]}
        hawk_authenticate.side_effect = Exception('asdf')
        urls = [
            '/api/v1/get-companies-house-company-numbers',
            '/api/v1/get-company-countries-and-sectors-of-interest',
            '/api/v1/get-company-countries-of-interest',
            '/api/v1/get-company-export-countries',
            '/api/v1/get-company-sectors-of-interest',
            '/api/v1/get-datahub-company-ids',
            '/api/v1/get-datahub-company-ids-to-companies-house-company-numbers',
            '/api/v1/get-sectors',
        ]
        for url in urls:
            response =  self.client.get(url)
            self.assertEqual(response.status_code, 401, url)


@patch('authentication.hawk_authenticate')
class TestGetCompanyCountriesAndSectorsOfInterest(TestCase):

    def test(self, hawk_authenticate):
        schema = countries_and_sectors_of_interest.table_fields
        table_name = countries_and_sectors_of_interest.table_name
        url = '/api/v1/get-company-countries-and-sectors-of-interest'
        values = [
            [
                '9baf4ac5-6654-411d-8671-3e7118f5b49f',
                'CN',
                'Aerospace',
                'omis',
                '123',
                '2019-01-01T00:00:00'
            ],
            [
                'a8d7c4dc-9092-4f2d-8d5a-8a69da9d948c',
                'US',
                'Food',
                'omis',
                '345',
                '2019-01-02T00:00:00'
            ],
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
            'headers': [
                'companyId',
                'countryOfInterest',
                'sectorOfInterest',
                'source',
                'sourceId',
                'timestamp',
            ],
            'next': None,
            'values': values
        }
        self.assertEqual(response.json, expected)

        
@patch('authentication.hawk_authenticate')
class TestGetCompanyCountriesOfInterest(TestCase):

    def test(self, hawk_authenticate):
        schema = countries_of_interest.table_fields
        table_name = countries_of_interest.table_name
        url = '/api/v1/get-company-countries-of-interest'
        values = [
            ['asdf', 'CN', 'omis', '123', '2019-01-01T00:00:00'],
            ['asdf33', 'US', 'omis', '345', '2019-01-02T00:00:00'],
        ]
        with app.app.app_context():
            connection = get_db()
            connection.autocommit = True
            cursor = connection.cursor()
        sql = ''' create table {} {} '''.format(table_name, schema)
        cursor.execute(sql)
        sql = ''' insert into {} values (%s, %s, %s, %s, %s) '''.format(table_name)
        cursor.executemany(sql, values)
        response = self.client.get(url)
        expected = {
            'headers': [
                'companyId',
                'countryOfInterest',
                'source',
                'sourceId',
                'timestamp',
            ],
            'next': None,
            'values': values
        }
        self.assertEqual(response.json, expected)


@patch('authentication.hawk_authenticate')
class TestGetCompanyExportCountries(TestCase):

    def test(self, hawk_authenticate):
        schema = export_countries.table_fields
        table_name = export_countries.table_name
        url = '/api/v1/get-company-export-countries'
        values = [
            ['asdf', 'CN', 'omis', '123', '2019-01-01T00:00:00'],
            ['asdf33', 'US', 'omis', '345', '2019-01-02T00:00:00'],
        ]
        with app.app.app_context():
            connection = get_db()
            connection.autocommit = True
            cursor = connection.cursor()
        sql = ''' create table {} {} '''.format(table_name, schema)
        cursor.execute(sql)
        sql = ''' insert into {} values (%s, %s, %s, %s, %s) '''.format(table_name)
        cursor.executemany(sql, values)
        response = self.client.get(url)
        expected = {
            'headers': [
                'companyId',
                'exportCountry',
                'source',
                'sourceId',
                'timestamp',
            ],
            'next': None,
            'values': values
        }
        self.assertEqual(response.json, expected)        


