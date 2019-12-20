import datetime
import uuid
from unittest.mock import Mock, patch

import pytest

import app.etl.tasks.core.source_data_extraction
import app.etl.tasks.core.source_data_extraction as source_data_extraction
from app.db.db_utils import execute_statement


class SourceDataExtractBaseTestCase:

    __test__ = False

    def get_dataworkspace_config(self):
        return {
            self.dataset_id_config_key: '1',
            self.source_table_id_config_key: '2',
            'base_url': 'asdf',
            'hawk_client_id': 'hawk_client_id',
            'hawk_client_key': 'hawk_client_key',
        }

    @patch('app.etl.tasks.core.source_data_extraction.current_app')
    def test_stub_data(self, current_app, app_with_db):
        current_app.config = {'app': {'stub_source_data': True}}
        self.extractor.__call__()
        sql = 'select * from {}'.format(self.table_name)
        status = execute_statement(sql, raise_if_fail=True)
        assert int(status.rowcount) > 0

    @patch('app.etl.tasks.core.source_data_extraction.requests')
    @patch('app.etl.tasks.core.source_data_extraction.current_app')
    def test_data(self, current_app, requests, app_with_db):
        current_app.config = {
            'app': {'stub_source_data': False},
            'dataworkspace': self.get_dataworkspace_config(),
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


class ReferenceDatasetExtractBaseTestCase(SourceDataExtractBaseTestCase):

    __test__ = False

    def get_dataworkspace_config(self):
        return {
            self.group_slug_config_key: 'group-slug',
            self.reference_slug_config_key: 'reference-slug',
            'base_url': 'asdf',
            'hawk_client_id': 'hawk_client_id',
            'hawk_client_key': 'hawk_client_key',
        }


class TestExtractCountriesAndTerritoriesReferenceDataset(
    ReferenceDatasetExtractBaseTestCase
):

    __test__ = True
    group_slug_config_key = 'countries_and_territories_group_slug'
    reference_slug_config_key = 'countries_and_territories_reference_slug'
    expected_data = [
        ['AE-AZ', 'Abu Dhabi', 'Territory', None, None],
        ['AF', 'Afghanistan', 'Country', None, None],
        ['AO', 'Angola', 'Country', '1975-11-11', None],
    ]
    source_data = {
        'headers': ['ID', 'Name', 'Type', 'Start Date', 'End Date'],
        'next': None,
        'values': [
            ['AE-AZ', 'Abu Dhabi', 'Territory', None, None],
            ['AF', 'Afghanistan', 'Country', None, None],
            ['AO', 'Angola', 'Country', '1975-11-11', None],
        ],
    }

    table_name = 'reference_countries_and_territories'
    extractor = (
        source_data_extraction.extract_countries_and_territories_reference_dataset
    )


class TestExtractDatahubCompany(SourceDataExtractBaseTestCase):

    __test__ = True
    dataset_id_config_key = 'datahub_companies_dataset_id'
    expected_data = [
        ['c31e4492-1f16-48a2-8c5e-8c0334d959a3', 'asdf', 'Food'],
        ['d0af8e52-ff34-4088-98e3-d2d22cd250ae', 'asdf2', 'Aerospace'],
    ]
    source_data = {
        'headers': ['id', 'company_number', 'sector', 'extra_field'],
        'next': None,
        'values': [
            ['c31e4492-1f16-48a2-8c5e-8c0334d959a3', 'asdf', 'Food', 'extra'],
            ['d0af8e52-ff34-4088-98e3-d2d22cd250ae', 'asdf2', 'Aerospace', 'extra'],
        ],
    }
    source_table_id_config_key = 'datahub_companies_source_table_id'
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
        'headers': ['id', 'company_id', 'country_iso_alpha2_code', 'extraField'],
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
        'headers': ['id', 'company_id', 'country_iso_alpha2_code', 'extra_field'],
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
        [
            'a8cb910f-51df-4d8e-a953-01c0be435d36',
            '7cd493ec-8e1c-4bbc-a0ba-ebd8fd118381',
            '05b2acd6-21cb-4a98-a857-d5ff773db4ff',
            '2019-01-01 01:00:00',
            '2019-01-01 00:00:00',
        ],
        [
            'c31e4492-1f16-48a2-8c5e-8c0334d959a3',
            '0774cc83-11e7-4100-8631-3b8b0998c514',
            '8ef278b1-0bde-4f25-8279-36f9ba05198d',
            '2019-01-02 02:00:00',
            '2019-01-02 00:00:00',
        ],
    ]
    source_data = {
        'headers': [
            'id',
            'event_id',
            'company_id',
            'created_on',
            'interaction_date',
            'extra_field',
        ],
        'values': [
            [
                'a8cb910f-51df-4d8e-a953-01c0be435d36',
                '7cd493ec-8e1c-4bbc-a0ba-ebd8fd118381',
                '05b2acd6-21cb-4a98-a857-d5ff773db4ff',
                '2019-01-01 01:00:00',
                '2019-01-01',
                'extra',
            ],
            [
                'c31e4492-1f16-48a2-8c5e-8c0334d959a3',
                '0774cc83-11e7-4100-8631-3b8b0998c514',
                '8ef278b1-0bde-4f25-8279-36f9ba05198d',
                '2019-01-02 02:00:00',
                '2019-01-02',
                'extra',
            ],
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
        'headers': ['company_id', 'market', 'created_date', 'id', 'sector'],
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
        'headers': ['id', 'company_id', 'country', 'timestamp'],
        'values': [
            ['23f66b0e-05be-40a5-9bf2-fa44dc7714a8', 'asdf', 'IT', '2019-01-01 1:00'],
            ['f50d892d-388a-405b-9e30-16b9971ac0d4', 'ffff', 'GO', '2019-01-02 18:00'],
        ],
        'next': None,
    }
    source_table_id_config_key = 'export_wins_source_table_id'
    table_name = 'export_wins'
    extractor = app.etl.tasks.core.source_data_extraction.extract_export_wins


class TestGetHawkHeaders:
    @patch('app.etl.tasks.core.source_data_extraction.mohawk')
    def test_if_https_is_false_change_url_to_http(self, mohawk):
        url = 'https://something'
        client_id = 'client_id'
        client_key = 'client_key'
        credentials = {'id': client_id, 'key': client_key, 'algorithm': 'sha256'}
        source_data_extraction.get_hawk_headers(url, client_id, client_key, https=False)
        mohawk.Sender.assert_called_once_with(
            credentials=credentials,
            url='http://something',
            method='GET',
            content='',
            content_type='',
        )


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

    @patch('app.etl.tasks.core.source_data_extraction.sql_alchemy')
    def test_rollback_when_there_is_an_error(self, mock_alchemy, app_with_db):
        connection = Mock()
        transaction = Mock()
        connection.begin.return_value = transaction
        mock_alchemy.engine.connect.return_value = connection
        transaction.commit.side_effect = Exception('some exception')
        sql = 'create table {} (a varchar(100), b integer)'.format(self.table_name)
        execute_statement(sql, raise_if_fail=True)
        sql = '''insert into {} values ('x', 3)'''.format(self.table_name)
        execute_statement(sql, raise_if_fail=True)

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
