import etl.tasks.core.source_data_extraction as extraction
from unittest.mock import Mock, patch
from tests.TestCase import TestCase
from etl.tasks.core.source_data_extraction import populate_table
from utils.sql import query_database
from app import app
from db import get_db


class TestExtractDatahubCompany(TestCase):
    @patch('etl.tasks.core.source_data_extraction.populate_table')
    def test(self, populate_table):
        schema = {
            'columns': ('id uuid', 'company_number varchar(50)', 'sector varchar(50)'),
            'primary_key': 'id',
        }
        data = {
            'headers': ['id', 'companyNumber', 'sector'],
            'values': [
                ['c31e4492-1f16-48a2-8c5e-8c0334d959a3', 'asdf', 'Food'],
                ['d0af8e52-ff34-4088-98e3-d2d22cd250ae', 'asdf2', 'Aerospace'],
            ],
        }
        table_name = 'datahub_company'
        url = 'http://{}/{}'.format(
            app.config['DATAWORKSPACE_HOST'], 'api/v1/dataset/datahub-company-dataset'
        )
        with app.app_context():
            output = extraction.extract_datahub_company_dataset()
        populate_table.assert_called_once_with(schema, table_name, url, stub_data=data)
        self.assertEqual(output, populate_table.return_value)


class TestExtractDatahubExportCountries(TestCase):
    @patch('etl.tasks.core.source_data_extraction.populate_table')
    def test(self, populate_table):
        data = {
            'headers': ['company_id', 'country', 'id'],
            'values': [
                ['c31e4492-1f16-48a2-8c5e-8c0334d959a3', 'US', 1],
                ['d0af8e52-ff34-4088-98e3-d2d22cd250ae', 'MY', 2],
            ],
        }
        schema = {
            'columns': ('company_id uuid', 'country varchar(2)', 'id int',),
            'primary_key': 'id',
        }
        table_name = 'datahub_export_countries'
        url = 'http://{}/{}'.format(
            app.config['DATAWORKSPACE_HOST'], 'api/v1/dataset/datahub-export-countries'
        )
        with app.app_context():
            output = extraction.extract_datahub_export_countries()
        populate_table.assert_called_once_with(schema, table_name, url, stub_data=data)
        self.assertEqual(output, populate_table.return_value)


class TestExtractDatahubFutureInterestCountries(TestCase):
    @patch('etl.tasks.core.source_data_extraction.get_hawk_headers')
    @patch('etl.tasks.core.source_data_extraction.populate_table')
    @patch('etl.tasks.core.source_data_extraction.requests')
    def test(self, requests, populate_table, get_hawk_headers):
        data = {
            'headers': ['companyId', 'country', 'id'],
            'values': [
                ['c31e4492-1f16-48a2-8c5e-8c0334d959a3', 'CN', 1],
                ['d0af8e52-ff34-4088-98e3-d2d22cd250ae', 'DE', 2],
            ],
        }
        schema = {
            'columns': ('company_id uuid', 'country varchar(2)', 'id int'),
            'primary_key': 'id',
        }
        table_name = 'datahub_future_interest_countries'
        url = 'http://{}/{}'.format(
            app.config['DATAWORKSPACE_HOST'],
            'api/v1/dataset/datahub-future-interest-countries',
        )
        with app.app_context():
            output = extraction.extract_datahub_future_interest_countries()
        populate_table.assert_called_once_with(schema, table_name, url, stub_data=data)
        self.assertEqual(output, populate_table.return_value)


class TestExtractDatahubInteractions(TestCase):
    @patch('etl.tasks.core.source_data_extraction.get_hawk_headers')
    @patch('etl.tasks.core.source_data_extraction.populate_table')
    @patch('etl.tasks.core.source_data_extraction.requests')
    def test(self, requests, populate_table, get_hawk_headers):
        data = {
            'headers': ['companyId', 'country', 'id'],
            'values': [
                ['c31e4492-1f16-48a2-8c5e-8c0334d959a3', 'CN', 1],
                ['d0af8e52-ff34-4088-98e3-d2d22cd250ae', 'DE', 2],
            ],
        }
        schema = {
            'columns': ('company_id uuid', 'country varchar(2)', 'id int',),
            'primary_key': 'id',
        }
        table_name = 'datahub_interactions'
        url = 'http://{}/{}'.format(
            app.config['DATAWORKSPACE_HOST'], 'api/v1/dataset/datahub-interactions'
        )
        with app.app_context():
            output = extraction.extract_datahub_interactions()
        populate_table.assert_called_once_with(schema, table_name, url, stub_data=data)
        self.assertEqual(output, populate_table.return_value)


class TestExtractDatahubOmis(TestCase):
    @patch('etl.tasks.core.source_data_extraction.get_hawk_headers')
    @patch('etl.tasks.core.source_data_extraction.populate_table')
    @patch('etl.tasks.core.source_data_extraction.requests')
    def test(self, requests, populate_table, get_hawk_headers):
        data = {
            'headers': ['companyId', 'country', 'createdOn', 'id', 'sector'],
            'values': [
                [
                    'c31e4492-1f16-48a2-8c5e-8c0334d959a3',
                    'CN',
                    '2018-01-01',
                    'e84de2c0-fe7a-41fc-ba1d-5885925ff3ca',
                    'Aerospace',
                ],
                [
                    'd0af8e52-ff34-4088-98e3-d2d22cd250ae',
                    'DE',
                    '2018-01-02',
                    'a3cc5ef5-0ec0-491a-aa48-48656d66e662',
                    'Food',
                ],
            ],
        }
        schema = {
            'columns': (
                'company_id uuid',
                'country varchar(2)',
                'created_on timestamp',
                'id uuid',
                'sector varchar(200)',
            ),
            'primary_key': 'id',
        }
        table_name = 'omis'
        url = 'http://{}/{}'.format(
            app.config['DATAWORKSPACE_HOST'], 'api/v1/omis-dataset'
        )
        with app.app_context():
            output = extraction.extract_datahub_omis_dataset()
        populate_table.assert_called_once_with(schema, table_name, url, stub_data=data)
        self.assertEqual(output, populate_table.return_value)


class TestExtractDatahubSectors(TestCase):
    @patch('etl.tasks.core.source_data_extraction.get_hawk_headers')
    @patch('etl.tasks.core.source_data_extraction.populate_table')
    @patch('etl.tasks.core.source_data_extraction.requests')
    def test(self, requests, populate_table, get_hawk_headers):
        data = {
            'headers': ['id', 'sector'],
            'values': [
                ['c3467472-3a97-4359-91f4-f860597e1837', 'Aerospace'],
                ['698d0cc3-ce8e-453b-b3c4-99818c5a9070', 'Food'],
            ],
        }
        schema = {
            'columns': ('id uuid', 'sector varchar(200)'),
            'primary_key': 'id',
        }
        table_name = 'datahub_sector'
        url = 'http://{}/{}'.format(
            app.config['DATAWORKSPACE_HOST'], 'api/v1/datahub-sectors-dataset'
        )
        with app.app_context():
            output = extraction.extract_datahub_sectors()
        populate_table.assert_called_once_with(schema, table_name, url, stub_data=data)
        self.assertEqual(output, populate_table.return_value)


class TestExtractExportWins(TestCase):
    @patch('etl.tasks.core.source_data_extraction.get_hawk_headers')
    @patch('etl.tasks.core.source_data_extraction.populate_table')
    @patch('etl.tasks.core.source_data_extraction.requests')
    def test(self, requests, populate_table, get_hawk_headers):
        data = {
            'headers': ['id', 'companyId', 'timestamp'],
            'values': [
                [
                    '23f66b0e-05be-40a5-9bf2-fa44dc7714a8',
                    'asdf',
                    'IT',
                    '2019-01-01 1:00',
                ],
                [
                    'f50d892d-388a-405b-9e30-16b9971ac0d4',
                    'ffff',
                    'GO',
                    '2019-01-02 18:00',
                ],
            ],
        }
        schema = {
            'columns': (
                'id uuid',
                'company_id varchar(12)',
                'country varchar(2)',
                'timestamp timestamp',
            ),
            'primary_key': 'id',
        }
        table_name = 'export_wins'
        url = 'http://{}/{}'.format(
            app.config['DATAWORKSPACE_HOST'], 'api/v1/export-wins'
        )
        with app.app_context():
            output = extraction.extract_export_wins()
        populate_table.assert_called_once_with(schema, table_name, url, stub_data=data)
        self.assertEqual(output, populate_table.return_value)


class TestPopulateTable(TestCase):
    def setUp(self):
        super().setUp()
        self.schema = {
            'columns': ['a varchar(100)', 'b integer'],
            'primary_key': ('a',),
        }
        self.table_name = 'existing_table'
        self.url = 'some_url'
        self.stub_data = {'headers': ['a', 'b'], 'values': [(0, 0), (1, 1)]}
        self.connection = Mock()

    @patch('etl.tasks.core.source_data_extraction.get_db')
    @patch('etl.tasks.core.source_data_extraction.sql_utils')
    def test_copies_table_to_backup_if_it_exists(self, sql_utils, get_db):
        get_db.return_value = self.connection
        self.connection.cursor.return_value = Mock(rowcount=1)
        sql_utils.table_exists.return_value = True
        with app.app_context():
            populate_table(
                self.schema, self.table_name, self.url, stub_data=self.stub_data
            )
        sql_utils.drop_table.assert_called_once_with(
            self.connection, 'existing_table_backup'
        )
        sql_utils.rename_table.assert_called_once_with(
            self.connection, 'existing_table', 'existing_table_backup'
        )

    @patch('etl.tasks.core.source_data_extraction.get_db')
    @patch('etl.tasks.core.source_data_extraction.requests')
    def test_if_stubbed_data_passed_use_it_instead_of_real_data(self, requests, get_db):
        with app.app_context():
            populate_table(
                self.schema, self.table_name, self.url, stub_data=self.stub_data
            )
        requests.assert_not_called()

    @patch('etl.tasks.core.source_data_extraction.get_db')
    @patch('etl.tasks.core.source_data_extraction.get_hawk_headers')
    @patch('etl.tasks.core.source_data_extraction.requests')
    def test_no_stubbed_data(self, requests, get_hawk_headers, get_db):
        headers = Mock()
        get_hawk_headers.return_value = headers
        with app.app_context():
            populate_table(
                self.schema, self.table_name, self.url,
            )
        requests.get.assert_called_once_with(self.url, headers=headers)

    @patch('etl.tasks.core.source_data_extraction.get_db')
    @patch('etl.tasks.core.source_data_extraction.sql_utils')
    def test_if_table_exists_and_insert_fails_fallback(self, sql_utils, get_db):
        get_db.return_value = self.connection
        sql_utils.table_exists.return_value = True
        cursor = self.connection.cursor.return_value
        cursor.execute.side_effect = Exception('asdf')
        with app.app_context():
            populate_table(
                self.schema, self.table_name, self.url, stub_data=self.stub_data,
            )
        sql_utils.rename_table.assert_called_with(
            self.connection, '{}_backup'.format(self.table_name), self.table_name,
        )

    def test_populates_table_correctly(self):
        schema = {
            'columns': ('a integer', 'b varchar(100)'),
            'primary_key': ('a',),
        }
        table_name = 'test_table'
        url = 'test_url'
        stub_data = {
            'headers': ['a', 'b'],
            'values': [(0, 0), (1, 1)],
        }

        with app.app_context():
            populate_table(schema, table_name, url, stub_data=stub_data)

        with app.app_context():
            connection = get_db()
            sql = 'select * from test_table'
            df = query_database(connection, sql)

        self.assertEqual([c for c in df.columns], ['a', 'b'])
        self.assertEqual(list(df.values[0]), [0, '0'])
        self.assertEqual(list(df.values[1]), [1, '1'])
