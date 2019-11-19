import pandas as pd
from unittest.mock import Mock, patch
from tests.TestCase import TestCase
from etl.extraction import extract_datahub_interactions, extract_export_wins, populate_table
from utils.sql import query_database
from app import app
from db import get_db

# TODO: tests for other extracts

class TestExtractExportWins(TestCase):

    def test(self):
        data = {
            'headers': ['id', 'companyId', 'timestamp'],
            'values': [
                ['23f66b0e-05be-40a5-9bf2-fa44dc7714a8', 'asdf', 'IT', '2019-01-01 01:00:00'],
                ['f50d892d-388a-405b-9e30-16b9971ac0d4', 'ffff', 'GO', '2019-01-02 18:00:00']
            ]
        }
        # requests.get.json.return_value = data
        with app.app_context():
            output = extract_export_wins()

        sql = ''' select * from export_wins '''
        with app.app_context():
            connection = get_db()
        df = query_database(connection, sql)
        df['timestamp'] = df['timestamp'].astype(str)
        df_expected = pd.DataFrame(data['values'])
        self.assertTrue((df.values == df_expected.values).all())


class TestExtractDatahubInteractions(TestCase):

    @patch('etl.extraction.get_hawk_headers')
    @patch('etl.extraction.populate_table')
    @patch('etl.extraction.requests')
    def test(self, requests, populate_table, get_hawk_headers):
        with app.app_context():
            output = extract_datahub_interactions()
        populate_table.assert_called_once()
        self.assertEqual(output, populate_table.return_value)
        

class TestPopulateTable(TestCase):

    def setUp(self):
        super().setUp()
        self.schema = {'columns': ['a varchar(100)', 'b integer'], 'primary_key': ('a', )}
        self.table_name = 'existing_table'
        self.url = 'some_url'
        self.stub_data = {'headers': ['a', 'b'], 'values': [(0, 0), (1, 1)]}
        self.connection = Mock()
        

    @patch('etl.extraction.get_db')
    @patch('etl.extraction.sql_utils')
    def test_copies_table_to_backup_if_it_exists(self, sql_utils, get_db):
        get_db.return_value = self.connection
        sql_utils.table_exists.return_value = True
        with app.app_context():
            output = populate_table(
                self.schema,
                self.table_name,
                self.url,
                stub_data=self.stub_data
            )
        sql_utils.drop_table.assert_called_once_with(self.connection, 'existing_table_backup')
        sql_utils.rename_table.assert_called_once_with(
            self.connection,
            'existing_table',
            'existing_table_backup'
        )

    @patch('etl.extraction.get_db')        
    @patch('etl.extraction.requests')
    def test_if_stubbed_data_passed_use_it_instead_of_real_data(self, requests, get_db):
        with app.app_context():
            output = populate_table(
                self.schema,
                self.table_name,
                self.url,
                stub_data=self.stub_data
            )
        requests.assert_not_called()

    @patch('etl.extraction.get_db')        
    @patch('etl.extraction.get_hawk_headers')
    @patch('etl.extraction.requests')
    def test_no_stubbed_data(self, requests, get_hawk_headers, get_db):
        headers = Mock()
        get_hawk_headers.return_value = headers
        with app.app_context():
            output = populate_table(
                self.schema,
                self.table_name,
                self.url,
            )
        requests.get.assert_called_once_with(self.url, headers=headers)

    @patch('etl.extraction.get_db')
    @patch('etl.extraction.sql_utils')
    def test_if_table_exists_and_insert_fails_fallback(self, sql_utils, get_db):
        get_db.return_value = self.connection
        sql_utils.table_exists.return_value = True
        cursor = self.connection.cursor.return_value
        cursor.execute.side_effect = Exception('asdf')
        with app.app_context():
            output = populate_table(
                self.schema,
                self.table_name,
                self.url,
                stub_data=self.stub_data,
            )
        sql_utils.rename_table.assert_called_with(
            self.connection,
            '{}_backup'.format(self.table_name),
            self.table_name,
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
            output = populate_table(schema ,table_name, url, stub_data=stub_data)

        
        with app.app_context():
            connection = get_db()
            sql = 'select * from test_table'
            df = query_database(connection, sql)

        self.assertEqual([c for c in df.columns], ['a', 'b'])
        self.assertEqual(list(df.values[0]), [0, '0'])
        self.assertEqual(list(df.values[1]), [1, '1'])
        
