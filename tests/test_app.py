import datetime
from unittest.mock import patch

import mohawk

import pandas as pd

import pytest

import app

from db import get_db

from etl.tasks.core import (
    countries_and_sectors_of_interest,
    countries_of_interest,
    export_countries,
)

from tests.TestCase import TestCase


class TestDataFlowRequest(TestCase):
    def request_data(self, client_id, client_key):
        url = 'http://localhost/api/v1/' 'get-company-countries-and-sectors-of-interest'
        algorithm = 'sha256'
        method = 'GET'
        content = ''
        content_type = ''
        credentials = {
            'id': client_id,
            'key': client_key,
            'algorithm': algorithm,
        }
        sender = mohawk.Sender(
            credentials=credentials,
            url=url,
            method=method,
            content=content,
            content_type=content_type,
        )
        headers = {
            'Authorization': sender.request_header,
            'Content-Type': content_type,
        }
        return self.client.get(url, headers=headers)

    @patch('app.query_database')
    def test_hawk_authenticated_request(self, query_database):
        users = [
            ('dataflow_client_id', 'dataflow_client_key'),
        ]
        app.create_users_table(users)
        client_id = 'dataflow_client_id'
        client_key = 'dataflow_client_key'
        query_database.return_value = pd.DataFrame([(0, 0), (1, 1)], columns=['a', 'b'])
        response = self.request_data(client_id, client_key)
        self.assertEqual(response.status_code, 200)

    def test_hawk_unauthenticated_request(self):
        users = [
            ('dataflow_client_id', 'dataflow_client_key'),
        ]
        app.create_users_table(users)
        client_id = 'dataflow_client_id'
        client_key = 'wrong_key'
        response = self.request_data(client_id, client_key)
        self.assertEqual(response.status_code, 401)


class TestHawkAuthentication(TestCase):
    @patch('app.query_database')
    @patch('app.to_web_dict')
    @patch('authentication.hawk_authenticate')
    def test_hawk_authentication(self, hawk_authenticate, to_web_dict, query_database):
        to_web_dict.return_value = {
            'headers': ['header1', 'header2'],
            'values': [[1, 2], [3, 4]],
        }
        hawk_authenticate.side_effect = Exception('asdf')
        urls = [
            '/api/v1/get-companies-house-company-numbers',
            '/api/v1/get-company-countries-and-sectors-of-interest',
            '/api/v1/get-company-countries-of-interest',
            '/api/v1/get-company-export-countries',
            '/api/v1/get-company-sectors-of-interest',
            '/api/v1/get-datahub-company-ids',
            '/api/v1/' 'get-datahub-company-ids-to-companies-house-company-numbers',
            '/api/v1/get-sectors',
        ]
        for url in urls:
            response = self.client.get(url)
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
                '2019-01-01T00:00:00',
            ],
            [
                'a8d7c4dc-9092-4f2d-8d5a-8a69da9d948c',
                'US',
                'Food',
                'omis',
                '345',
                '2019-01-02T00:00:00',
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
            'values': values,
        }
        self.assertEqual(response.json, expected)


@patch('authentication.hawk_authenticate')
class TestGetCompanyCountriesOfInterest(TestCase):
    maxDiff = None

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
            'values': values,
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
            'values': values,
        }
        self.assertEqual(response.json, expected)


@pytest.mark.skip(reason="Need to fix mock login")
class TestGetIndex(TestCase):
    @patch('app.login_required')
    @patch('app.render_template')
    def test_last_updated_passed_to_template(self, render_template, login_required):
        with app.app.app_context():
            with get_db() as connection:
                with connection.cursor() as cursor:
                    sql = 'create table if not exists ' 'etl_runs (timestamp timestamp)'
                    cursor.execute(sql)
                    sql = 'insert into etl_runs values (%s)'
                    values = [
                        ('2019-01-01 01:00',),
                        ('2019-01-01 02:00',),
                    ]
                    cursor.executemany(sql, values)

            with app.app.test_request_context():
                app.get_index()
        render_template.assert_called_once_with(
            'index.html', last_updated='2019-01-01 02:00:00',
        )

    @patch('app.login_required')
    @patch('app.render_template')
    def test_returns_message_if_there_are_no_prior_runs(
        self, render_template, login_required
    ):
        with app.app.test_request_context():
            app.get_index()
        render_template.assert_called_once_with(
            'index.html', last_updated='Database not yet initialised',
        )


@patch('authentication.hawk_authenticate')
@patch('app.populate_database_task')
class TestPopulateDatabase(TestCase):
    def test_called_without_drop_table_in_request(
        self, populate_database_task, hawk_authenticate
    ):
        with app.app.test_request_context():
            output = app.populate_database()
        populate_database_task.delay.assert_called_once_with(False)
        self.assertEqual(
            output, {'status': 200, 'message': 'started populate_database task'}
        )

    def test_called_with_drop_table_in_request(
        self, populate_database_task, hawk_authenticate
    ):
        with app.app.test_request_context() as request:
            request.request.args = {'drop-table': ''}
            output = app.populate_database()
        populate_database_task.delay.assert_called_once_with(True)
        self.assertEqual(
            output, {'status': 200, 'message': 'started populate_database task'}
        )

    @patch('app.datetime')
    def test_creates_etl_status_table_if_it_does_not_exist(
        self, mock_datetime, populate_database_task, hawk_authenticate
    ):
        mock_datetime.datetime.now.return_value = datetime.datetime(2019, 1, 1, 1)
        with app.app.test_request_context() as request:
            request.request.args = {'drop-table': ''}
            app.populate_database()
        with app.app.app_context():
            with get_db() as connection:
                with connection.cursor() as cursor:
                    sql = 'select * from etl_status'
                    cursor.execute(sql)
                    rows = cursor.fetchall()
        populate_database_task.delay.assert_called_once()
        self.assertEqual(rows, [('RUNNING', datetime.datetime(2019, 1, 1, 1))])

    def test_if_force_update_rerun_task(
        self, populate_database_task, hawk_authenticate
    ):
        with app.app.test_request_context() as request:
            request.request.args = {'force-update': ''}
            app.populate_database()
        populate_database_task.delay.assert_called_once()

    def test_if_force_update_rerun_while_another_task_is_running(
        self, populate_database_task, hawk_authenticate
    ):
        with app.app.app_context():
            with get_db() as connection:
                with connection.cursor() as cursor:
                    sql = (
                        'create table if not exists etl_status ('
                        'status varchar(100), timestamp timestamp)'
                    )
                    cursor.execute(sql)
                    sql = 'insert into etl_status values (%s, %s)'
                    cursor.execute(sql, ['RUNNING', '2019-01-01 01:00'])
        with app.app.test_request_context() as request:
            request.request.args = {'force-update': ''}
            app.populate_database()
        populate_database_task.delay.assert_called_once()

    def test_does_not_rerun_while_another_task_is_running(
        self, populate_database_task, hawk_authenticate
    ):
        with app.app.app_context():
            with get_db() as connection:
                with connection.cursor() as cursor:
                    sql = (
                        'create table if not exists etl_status ('
                        'status varchar(100), timestamp timestamp)'
                    )
                    cursor.execute(sql)
                    sql = 'insert into etl_status values (%s, %s)'
                    cursor.execute(sql, ['RUNNING', '2019-01-01 01:00'])
        with app.app.test_request_context():
            app.populate_database()
        populate_database_task.delay.assert_not_called()

    def test_reruns_task_if_last_was_succesful(
        self, populate_database_task, hawk_authenticate
    ):
        with app.app.app_context():
            with get_db() as connection:
                with connection.cursor() as cursor:
                    sql = (
                        'create table if not exists etl_status ('
                        'status varchar(100), timestamp timestamp)'
                    )
                    cursor.execute(sql)
                    sql = 'insert into etl_status values (%s, %s)'
                    cursor.execute(sql, ['SUCCESS', '2019-01-01 01:00'])
        with app.app.test_request_context():
            app.populate_database()
        populate_database_task.delay.assert_called_once()
