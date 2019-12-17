import datetime
import uuid
from unittest.mock import Mock, patch

import pytest

import app.etl.tasks.core.source_data_extraction
import app.etl.tasks.core.source_data_extraction as source_data_extraction
from app.db.db_utils import execute_statement


class SourceDataExtractBaseTestCase:

    __test__ = False

    @patch('app.etl.tasks.core.source_data_extraction.current_app')
    def test_stub_data(self, current_app, app_with_db):
        current_app.config = {'app': {'stub_source_data': True}}
        # with app.app_context():
        self.extractor.__call__()
        sql = 'select * from {}'.format(self.table_name)
        status = execute_statement(sql, raise_if_fail=True)
        assert int(status.rowcount) > 0

    @patch('app.etl.tasks.core.source_data_extraction.requests')
    @patch('app.etl.tasks.core.source_data_extraction.current_app')
    def test_data(self, current_app, requests, app_with_db):
        current_app.config = {
            'app': {'stub_source_data': False},
            'dataworkspace': {
                self.dataset_id_config_key: '1',
                self.source_table_id_config_key: '2',
                'host': 'asdf',
                'hawk_client_id': 'hawk_client_id',
                'hawk_client_key': 'hawk_client_key',
            },
        }
        response = Mock()
        response.json.return_value = self.source_data
        requests.get.return_value = response
        response = self.extractor.__call__()
        sql = 'select * from {}'.format(self.table_name)
        status = execute_statement(sql, raise_if_fail=True)
        assert int(status.rowcount) > 0
        data = status.fetchall()
        data = list(
            map(
                lambda d: [
                    str(x)
                    if type(x) in (uuid.UUID, datetime.datetime, datetime.date)
                    else x
                    for x in d
                ],
                data,
            )
        )
        assert data == self.expected_data


class TestExtractDatahubCompany(SourceDataExtractBaseTestCase):

    __test__ = True
    dataset_id_config_key = 'datahub_company_dataset_id'
    expected_data = [
        ['c31e4492-1f16-48a2-8c5e-8c0334d959a3', 'asdf', 'Food'],
        ['d0af8e52-ff34-4088-98e3-d2d22cd250ae', 'asdf2', 'Aerospace'],
    ]
    source_data = {
        'headers': ['id', 'companyNumber', 'sector', 'extraField'],
        'next': None,
        'values': [
            ['c31e4492-1f16-48a2-8c5e-8c0334d959a3', 'asdf', 'Food', 'extra'],
            ['d0af8e52-ff34-4088-98e3-d2d22cd250ae', 'asdf2', 'Aerospace', 'extra'],
        ],
    }
    source_table_id_config_key = 'datahub_company_source_table_id'
    table_name = 'datahub_company'
    extractor = source_data_extraction.extract_datahub_company_dataset


class TestExtractDatahubExportCountries(SourceDataExtractBaseTestCase):
    __test__ = True
    dataset_id_config_key = 'datahub_export_countries_dataset_id'
    expected_data = [
        ['c31e4492-1f16-48a2-8c5e-8c0334d959a3', 'SK', 0],
        ['d0af8e52-ff34-4088-98e3-d2d22cd250ae', 'SD', 1],
    ]
    source_data = {
        'headers': ['id', 'companyId', 'country', 'extraField'],
        'next': None,
        'values': [
            [0, 'c31e4492-1f16-48a2-8c5e-8c0334d959a3', 'SK', 'extra'],
            [1, 'd0af8e52-ff34-4088-98e3-d2d22cd250ae', 'SD', 'extra'],
        ],
    }
    source_table_id_config_key = 'datahub_export_countries_source_table_id'
    table_name = 'datahub_export_countries'
    extractor = source_data_extraction.extract_datahub_export_countries


class TestExtractDatahubFutureInterestCountries(SourceDataExtractBaseTestCase):
    __test__ = True
    dataset_id_config_key = 'datahub_future_interest_countries_dataset_id'
    expected_data = [
        ['c31e4492-1f16-48a2-8c5e-8c0334d959a3', 'SK', 0],
        ['d0af8e52-ff34-4088-98e3-d2d22cd250ae', 'SD', 1],
    ]
    source_data = {
        'headers': ['id', 'companyId', 'country', 'extraField'],
        'next': None,
        'values': [
            [0, 'c31e4492-1f16-48a2-8c5e-8c0334d959a3', 'SK', 'extra'],
            [1, 'd0af8e52-ff34-4088-98e3-d2d22cd250ae', 'SD', 'extra'],
        ],
    }
    source_table_id_config_key = 'datahub_future_interest_countries_source_table_id'
    table_name = 'datahub_future_interest_countries'
    extractor = source_data_extraction.extract_datahub_future_interest_countries


class TestExtractDatahubInteractions(SourceDataExtractBaseTestCase):
    __test__ = True
    dataset_id_config_key = 'datahub_interactions_dataset_id'
    expected_data = [
        ['c31e4492-1f16-48a2-8c5e-8c0334d959a3', 'CN', 1],
        ['d0af8e52-ff34-4088-98e3-d2d22cd250ae', 'DE', 2],
    ]
    source_data = {
        'headers': ['companyId', 'country', 'id'],
        'values': [
            ['c31e4492-1f16-48a2-8c5e-8c0334d959a3', 'CN', 1],
            ['d0af8e52-ff34-4088-98e3-d2d22cd250ae', 'DE', 2],
        ],
        'next': None,
    }
    source_table_id_config_key = 'datahub_interactions_source_table_id'
    table_name = 'datahub_interactions'
    extractor = source_data_extraction.extract_datahub_interactions


class TestExtractDatahubOmis(SourceDataExtractBaseTestCase):
    __test__ = True
    dataset_id_config_key = 'datahub_omis_dataset_id'
    expected_data = [
        [
            'c31e4492-1f16-48a2-8c5e-8c0334d959a3',
            'CN',
            '2018-01-01 00:00:00',
            'e84de2c0-fe7a-41fc-ba1d-5885925ff3ca',
            'Aerospace',
        ],
        [
            'd0af8e52-ff34-4088-98e3-d2d22cd250ae',
            'DE',
            '2018-01-02 00:00:00',
            'a3cc5ef5-0ec0-491a-aa48-48656d66e662',
            'Food',
        ],
    ]
    source_data = {
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
        'next': None,
    }
    source_table_id_config_key = 'datahub_omis_source_table_id'
    table_name = 'datahub_omis'
    extractor = app.etl.tasks.core.source_data_extraction.extract_datahub_omis


class TestExtractDatahubSectors(SourceDataExtractBaseTestCase):
    __test__ = True
    dataset_id_config_key = 'datahub_sectors_dataset_id'
    expected_data = [
        ['c3467472-3a97-4359-91f4-f860597e1837', 'Aerospace'],
        ['698d0cc3-ce8e-453b-b3c4-99818c5a9070', 'Food'],
    ]
    source_data = {
        'headers': ['id', 'sector'],
        'values': [
            ['c3467472-3a97-4359-91f4-f860597e1837', 'Aerospace'],
            ['698d0cc3-ce8e-453b-b3c4-99818c5a9070', 'Food'],
        ],
        'next': None,
    }
    source_table_id_config_key = 'datahub_sectors_source_table_id'
    table_name = 'datahub_sectors'
    extractor = app.etl.tasks.core.source_data_extraction.extract_datahub_sectors


class TestExtractExportWins(SourceDataExtractBaseTestCase):
    __test__ = True
    dataset_id_config_key = 'export_wins_dataset_id'
    expected_data = [
        ['23f66b0e-05be-40a5-9bf2-fa44dc7714a8', 'asdf', 'IT', '2019-01-01 01:00:00'],
        ['f50d892d-388a-405b-9e30-16b9971ac0d4', 'ffff', 'GO', '2019-01-02 18:00:00'],
    ]
    source_data = {
        'headers': ['id', 'companyId', 'country', 'timestamp'],
        'values': [
            ['23f66b0e-05be-40a5-9bf2-fa44dc7714a8', 'asdf', 'IT', '2019-01-01 1:00'],
            ['f50d892d-388a-405b-9e30-16b9971ac0d4', 'ffff', 'GO', '2019-01-02 18:00'],
        ],
        'next': None,
    }
    source_table_id_config_key = 'export_wins_source_table_id'
    table_name = 'export_wins'
    extractor = app.etl.tasks.core.source_data_extraction.extract_export_wins


class TestPopulateTable:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.schema = {
            'columns': [
                {'name': 'a', 'type': 'varchar(100)'},
                {'name': 'b', 'type': 'integer'},
            ],
            'primary_key': ('a',),
        }
        self.table_name = 'existing_table'
        self.url = 'some_url'
        self.stub_data = {'headers': ['a', 'b'], 'values': [(0, 0), (1, 1)]}

    @patch('app.etl.tasks.core.source_data_extraction.execute_statement')
    def test_rollback_when_there_is_an_error(self, mock_execute_statement, app_with_db):
        mock_execute_statement.side_effect = Exception('some exception')
        sql = 'create table {} (a varchar(100), b integer)'.format(self.table_name)
        execute_statement(sql, raise_if_fail=True)
        sql = '''insert into {} values ('x', 3)'''.format(self.table_name)
        execute_statement(sql, raise_if_fail=True)

        # with self.assertRaises(Exception):
        with pytest.raises(Exception):
            source_data_extraction.populate_table(
                self.stub_data, self.schema, self.table_name, overwrite=True,
            )

        sql = '''select * from {}'''.format(self.table_name)
        status = execute_statement(sql, raise_if_fail=True)
        rows = status.fetchall()

        assert rows == [('x', 3)]

    def test_if_overwrite_is_true_deletes_existing_data(self, app_with_db):
        sql = 'create table {} (a varchar(100), b integer)'.format(self.table_name)
        execute_statement(sql, raise_if_fail=True)
        sql = '''insert into {} values ('x', 3)'''.format(self.table_name)
        execute_statement(sql, raise_if_fail=True)

        data = {
            'headers': ['a', 'b'],
            'values': [('x', 0), ('y', 1)],
        }
        source_data_extraction.populate_table(
            data, self.schema, self.table_name, overwrite=True,
        )

        sql = '''select * from {}'''.format(self.table_name)
        status = execute_statement(sql, raise_if_fail=True)
        rows = status.fetchall()

        assert rows == data['values']

    def test_if_overwrite_is_false_upserts_to_table(self, app_with_db):
        sql = '''
        create table {} (
        a varchar(100),
        b integer, primary key (a)
        )'''.format(
            self.table_name
        )
        execute_statement(sql, raise_if_fail=True)
        sql = '''insert into {} values ('x', 3)'''.format(self.table_name)
        execute_statement(sql, raise_if_fail=True)

        data = {
            'headers': ['a', 'b'],
            'values': [('x', 0), ('y', 1)],
        }
        source_data_extraction.populate_table(
            data, self.schema, self.table_name, overwrite=False,
        )

        sql = '''select * from {}'''.format(self.table_name)
        status = execute_statement(sql, raise_if_fail=True)
        rows = status.fetchall()

        assert rows == [('x', 0), ('y', 1)]
