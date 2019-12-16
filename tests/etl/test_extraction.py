from unittest.mock import Mock, patch

import pytest

import app.etl.tasks.core.source_data_extraction as extraction
from app.db.db_utils import execute_query
from app.etl.tasks.core.source_data_extraction import populate_table


class TestExtractDatahubCompany:
    @patch('app.etl.tasks.core.source_data_extraction.populate_table')
    def test(self, populate_table, app):
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
        host = app.config['dataworkspace']['host']
        url = f"http://{host}/{'api/v1/dataset/datahub-company-dataset'}"
        output = extraction.extract_datahub_company_dataset()
        populate_table.assert_called_once_with(schema, table_name, url, stub_data=data)
        assert output == populate_table.return_value


class TestExtractDatahubExportCountries:
    @patch('app.etl.tasks.core.source_data_extraction.populate_table')
    def test(self, populate_table, app):
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
            app.config['dataworkspace']['host'],
            'api/v1/dataset/datahub-export-countries',
        )
        output = extraction.extract_datahub_export_countries()
        populate_table.assert_called_once_with(schema, table_name, url, stub_data=data)
        assert output == populate_table.return_value


class TestExtractDatahubFutureInterestCountries:
    @patch('app.etl.tasks.core.source_data_extraction.get_hawk_headers')
    @patch('app.etl.tasks.core.source_data_extraction.populate_table')
    @patch('app.etl.tasks.core.source_data_extraction.requests')
    def test(self, requests, populate_table, get_hawk_headers, app):
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
            app.config['dataworkspace']['host'],
            'api/v1/dataset/datahub-future-interest-countries',
        )
        output = extraction.extract_datahub_future_interest_countries()
        populate_table.assert_called_once_with(schema, table_name, url, stub_data=data)
        assert output == populate_table.return_value


class TestExtractDatahubInteractions:
    @patch('app.etl.tasks.core.source_data_extraction.get_hawk_headers')
    @patch('app.etl.tasks.core.source_data_extraction.populate_table')
    @patch('app.etl.tasks.core.source_data_extraction.requests')
    def test(self, requests, populate_table, get_hawk_headers, app):
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
            app.config['dataworkspace']['host'], 'api/v1/dataset/datahub-interactions'
        )
        output = extraction.extract_datahub_interactions()
        populate_table.assert_called_once_with(schema, table_name, url, stub_data=data)
        assert output == populate_table.return_value


class TestExtractDatahubOmis:
    @patch('app.etl.tasks.core.source_data_extraction.get_hawk_headers')
    @patch('app.etl.tasks.core.source_data_extraction.populate_table')
    @patch('app.etl.tasks.core.source_data_extraction.requests')
    def test(self, requests, populate_table, get_hawk_headers, app):
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
            app.config['dataworkspace']['host'], 'api/v1/omis-dataset'
        )
        output = extraction.extract_datahub_omis_dataset()
        populate_table.assert_called_once_with(schema, table_name, url, stub_data=data)
        assert output == populate_table.return_value


class TestExtractDatahubSectors:
    @patch('app.etl.tasks.core.source_data_extraction.get_hawk_headers')
    @patch('app.etl.tasks.core.source_data_extraction.populate_table')
    @patch('app.etl.tasks.core.source_data_extraction.requests')
    def test(self, requests, populate_table, get_hawk_headers, app):
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
            app.config['dataworkspace']['host'], 'api/v1/datahub-sectors-dataset'
        )
        output = extraction.extract_datahub_sectors()
        populate_table.assert_called_once_with(schema, table_name, url, stub_data=data)
        assert output == populate_table.return_value


class TestExtractExportWins:
    @patch('app.etl.tasks.core.source_data_extraction.get_hawk_headers')
    @patch('app.etl.tasks.core.source_data_extraction.populate_table')
    @patch('app.etl.tasks.core.source_data_extraction.requests')
    def test(self, requests, populate_table, get_hawk_headers, app):
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
            app.config['dataworkspace']['host'], 'api/v1/export-wins'
        )
        output = extraction.extract_export_wins()
        populate_table.assert_called_once_with(schema, table_name, url, stub_data=data)
        assert output == populate_table.return_value


@patch('app.etl.tasks.core.source_data_extraction.rename_table')
@patch('app.etl.tasks.core.source_data_extraction.drop_table')
@patch('app.etl.tasks.core.source_data_extraction.table_exists')
@patch('app.etl.tasks.core.source_data_extraction.execute_statement')
class TestPopulateTable:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.schema = {
            'columns': ['a varchar(100)', 'b integer'],
            'primary_key': ('a',),
        }
        self.table_name = 'existing_table'
        self.url = 'some_url'
        self.stub_data = {'headers': ['a', 'b'], 'values': [(0, 0), (1, 1)]}

    def test_copies_table_to_backup_if_it_exists(
        self, execute_statement, table_exists, drop_table, rename_table
    ):
        execute_statement.return_value = Mock(rowcount=1)
        table_exists.return_value = True
        populate_table(self.schema, self.table_name, self.url, stub_data=self.stub_data)
        drop_table.assert_called_once_with('existing_table_backup')
        rename_table.assert_called_once_with('existing_table', 'existing_table_backup')

    @patch('app.etl.tasks.core.source_data_extraction.requests')
    def test_if_stubbed_data_passed_use_it_instead_of_real_data(
        self, requests, execute_statement, table_exists, drop_table, rename_table
    ):
        populate_table(self.schema, self.table_name, self.url, stub_data=self.stub_data)
        requests.assert_not_called()

    @patch('app.etl.tasks.core.source_data_extraction.get_hawk_headers')
    @patch('app.etl.tasks.core.source_data_extraction.requests')
    def test_no_stubbed_data(
        self,
        requests,
        get_hawk_headers,
        execute_statement,
        table_exists,
        drop_table,
        rename_table,
    ):
        headers = Mock()
        get_hawk_headers.return_value = headers
        populate_table(
            self.schema, self.table_name, self.url,
        )
        requests.get.assert_called_once_with(self.url, headers=headers)

    def test_if_table_exists_and_insert_fails_fallback(
        self, execute_statement, table_exists, drop_table, rename_table
    ):
        table_exists.return_value = True
        execute_statement.side_effect = Exception('asdf')
        populate_table(
            self.schema, self.table_name, self.url, stub_data=self.stub_data,
        )
        rename_table.assert_called_with(
            f'{self.table_name}_backup', self.table_name,
        )


def test_populates_table_correctly(app_with_db):
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

    populate_table(schema, table_name, url, stub_data=stub_data)

    sql = 'select * from test_table'
    df = execute_query(sql)

    assert [c for c in df.columns] == ['a', 'b']
    assert list(df.values[0]) == [0, '0']
    assert list(df.values[1]) == [1, '1']
