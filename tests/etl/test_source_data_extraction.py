import uuid
from unittest.mock import Mock, patch

import pytest
import sqlalchemy.exc
from flask import current_app as flask_app

import app.db.models.external as models
from app.etl.tasks import source_data_extraction


@pytest.fixture()
def stub_data_on(app):
    app.config['app']['stub_source_data'] = True
    yield
    app.config['app']['stub_source_data'] = False


@pytest.fixture()
def stub_data_off(app):
    app.config['app']['stub_source_data'] = False
    yield
    app.config['app']['stub_source_data'] = True


class SourceDataExtractBaseTestCase:
    item_pk = 'id'
    extractor_name = None

    __test__ = False

    def get_dataworkspace_config(self):
        return {
            self.dataset_id_config_key: '1',
            self.source_table_id_config_key: '2',
            'base_url': 'asdf',
            'hawk_client_id': 'hawk_client_id',
            'hawk_client_key': 'hawk_client_key',
        }

    def test_stub_data(self, stub_data_on, app_with_db):
        extractor = self.extractor()
        response = extractor()
        number_of_rows = len(self.extractor.stub_data['values'])
        assert response == {
            'rows': number_of_rows,
            'status': 200,
            'extractor': self.extractor.name,
        }
        objects = self.extractor.model.query.all()
        assert len(objects) == number_of_rows

    @patch('app.etl.tasks.source_data_extraction.requests')
    def test_data(self, requests, app_with_db, stub_data_off):
        app_with_db.config['dataworkspace'] = self.get_dataworkspace_config()
        response = Mock()
        response.json.return_value = self.source_data
        requests.get.return_value = response
        extractor = self.extractor()
        response = extractor()
        number_of_rows = len(self.expected_data)
        assert response == {
            'rows': number_of_rows,
            'status': 200,
            'extractor': self.extractor.name,
        }
        for expected_item in self.expected_data:
            actual_object = self.extractor.model.query.filter_by(
                **{self.item_pk: expected_item[self.item_pk]}
            ).one()
            for k, v in expected_item.items():
                assert str(getattr(actual_object, k)) == str(v)


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


class TestExtractCountriesAndTerritoriesReferenceDataset(ReferenceDatasetExtractBaseTestCase):
    __test__ = True
    group_slug_config_key = 'countries_and_territories_group_slug'
    reference_slug_config_key = 'countries_and_territories_reference_slug'
    item_pk = 'country_iso_alpha2_code'
    expected_data = [
        {
            'country_iso_alpha2_code': 'AE-AZ',
            'name': 'Abu Dhabi',
            'type': 'Territory',
            'start_date': None,
            'end_date': None,
        },
        {
            'country_iso_alpha2_code': 'AF',
            'name': 'Afghanistan',
            'type': 'Country',
            'start_date': None,
            'end_date': None,
        },
        {
            'country_iso_alpha2_code': 'AO',
            'name': 'Angola',
            'type': 'Country',
            'start_date': '1975-11-11',
            'end_date': None,
        },
    ]
    source_data = {
        'headers': ['ID', 'Name', 'Type', 'Start date', 'End date'],
        'next': None,
        'values': [
            ['AE-AZ', 'Abu Dhabi', 'Territory', None, None],
            ['AF', 'Afghanistan', 'Country', None, None],
            ['AO', 'Angola', 'Country', '1975-11-11', None],
        ],
    }
    extractor = source_data_extraction.ExtractCountriesAndTerritoriesReferenceDataset


class TestExtractDatahubCompany(SourceDataExtractBaseTestCase):
    __test__ = True
    dataset_id_config_key = 'datahub_companies_dataset_id'
    item_pk = 'datahub_company_id'
    expected_data = [
        {
            'datahub_company_id': 'c31e4492-1f16-48a2-8c5e-8c0334d959a3',
            'companies_house_id': 'asdf',
            'sector': 'Food',
        },
        {
            'datahub_company_id': 'd0af8e52-ff34-4088-98e3-d2d22cd250ae',
            'companies_house_id': 'asdf2',
            'sector': 'Aerospace',
        },
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
    extractor = source_data_extraction.ExtractDatahubCompanyDataset


class TestExtractDatahubCompanyExportCountry(SourceDataExtractBaseTestCase):
    __test__ = True
    dataset_id_config_key = 'datahub_company_export_country_dataset_id'
    expected_data = [
        {
            'company_id': 'c31e4492-1f16-48a2-8c5e-8c0334d959a3',
            'country': 'Canada',
            'country_iso_alpha2_code': 'CA',
            'created_on': '2020-01-01 01:00:00',
            'company_export_country_id': '41a83926-b771-4a33-b3d2-e944fa35b366',
            'modified_on': '2020-01-01 02:00:00',
            'status': 'currently_exporting',
        },
        {
            'company_id': 'd0af8e52-ff34-4088-98e3-d2d22cd250ae',
            'country': 'Georgia',
            'country_iso_alpha2_code': 'GE',
            'created_on': '2020-01-02 01:00:00',
            'company_export_country_id': '6b9c0565-d007-48ae-888c-77638633b434',
            'modified_on': '2020-01-02 02:00:00',
            'status': 'future_interest',
        },
    ]
    item_pk = 'company_export_country_id'
    source_data = {
        'headers': [
            'company_id',
            'country',
            'country_iso_alpha2_code',
            'created_on',
            'id',
            'modified_on',
            'status',
        ],
        'next': None,
        'values': [
            [
                'c31e4492-1f16-48a2-8c5e-8c0334d959a3',
                'Canada',
                'CA',
                '2020-01-01 01:00:00',
                '41a83926-b771-4a33-b3d2-e944fa35b366',
                '2020-01-01 02:00:00',
                'currently_exporting',
            ],
            [
                'd0af8e52-ff34-4088-98e3-d2d22cd250ae',
                'Georgia',
                'GE',
                '2020-01-02 01:00:00',
                '6b9c0565-d007-48ae-888c-77638633b434',
                '2020-01-02 02:00:00',
                'future_interest',
            ],
        ],
    }
    source_table_id_config_key = 'datahub_company_export_country_source_table_id'
    extractor = source_data_extraction.ExtractDatahubCompanyExportCountry


class TestExtractDatahubCompanyExportCountryHistory(SourceDataExtractBaseTestCase):
    __test__ = True
    dataset_id_config_key = 'datahub_company_export_country_history_dataset_id'
    expected_data = [
        {
            'company_id': '6ef62b18-fe0f-4e0e-8cbf-3ca3399df788',
            'country_iso_alpha2_code': 'US',
            'country': 'united states',
            'history_date': '2020-01-01 01:00:00',
            'history_id': 'afd47aa2-b3fd-4c2e-a28b-44fef679c26e',
            'history_type': 'update',
            'status': 'not_interested',
        },
        {
            'company_id': '6a877ef4-d907-4dc0-a432-f140deabef2b',
            'country_iso_alpha2_code': 'CN',
            'country': 'china',
            'history_date': '2020-01-01 02:00:00',
            'history_id': '592ff565-88d0-43d7-b5d3-7e44fef6ed56',
            'history_type': 'insert',
            'status': 'currently_exporting',
        },
    ]
    item_pk = 'history_id'

    source_data = {
        'headers': [
            'company_id',
            'country_iso_alpha2_code',
            'country',
            'history_date',
            'id',
            'history_type',
            'status',
            'extraField',
        ],
        'next': None,
        'values': [
            [
                '6ef62b18-fe0f-4e0e-8cbf-3ca3399df788',
                'US',
                'united states',
                '2020-01-01 01:00:00',
                'afd47aa2-b3fd-4c2e-a28b-44fef679c26e',
                'update',
                'not_interested',
                'extra_field_1',
            ],
            [
                '6a877ef4-d907-4dc0-a432-f140deabef2b',
                'CN',
                'china',
                '2020-01-01 02:00:00',
                '592ff565-88d0-43d7-b5d3-7e44fef6ed56',
                'insert',
                'currently_exporting',
                'extra_field_2',
            ],
        ],
    }

    source_table_id_config_key = 'datahub_company_export_country_history_source_table_id'
    extractor = source_data_extraction.ExtractDatahubCompanyExportCountryHistory


class TestExtractDatahubInteractions(SourceDataExtractBaseTestCase):
    __test__ = True
    item_pk = 'datahub_interaction_id'
    dataset_id_config_key = 'datahub_interactions_dataset_id'
    expected_data = [
        {
            'datahub_interaction_id': '798c74ef-7de6-4c3a-aa46-51692c2093b8',
            'datahub_company_id': 'ad8ac56b-2a60-4100-9ab2-d868fce37c27',
            'notes': 'Test 1',
            'subject': 'Test Subject 1',
            'created_on': '2020-01-01 00:00:00',
        },
        {
            'datahub_interaction_id': 'dc3ee18d-bbd5-4c18-b107-d8ebbcbf650b',
            'datahub_company_id': '37be7f9d-9c2b-4e42-adc7-5d9ad9b66a97',
            'notes': 'Test 2',
            'subject': 'Test Subject 2',
            'created_on': '2020-01-02 00:00:00',
        },
    ]
    source_data = {
        'headers': ['id', 'company_id', 'interaction_notes', 'interaction_subject', 'created_on'],
        'values': [
            [
                '798c74ef-7de6-4c3a-aa46-51692c2093b8',
                'ad8ac56b-2a60-4100-9ab2-d868fce37c27',
                'Test 1',
                'Test Subject 1',
                '2020-01-01',
            ],
            [
                'dc3ee18d-bbd5-4c18-b107-d8ebbcbf650b',
                '37be7f9d-9c2b-4e42-adc7-5d9ad9b66a97',
                'Test 2',
                'Test Subject 2',
                '2020-01-02',
            ],
        ],
        'next': None,
    }
    source_table_id_config_key = 'datahub_interactions_source_table_id'
    extractor = source_data_extraction.ExtractDatahubInteractions


class TestExtractDatahubInteractionsExportCountry(SourceDataExtractBaseTestCase):
    __test__ = True
    dataset_id_config_key = 'datahub_interactions_export_country_dataset_id'
    expected_data = [
        {
            'country_name': 'Tajikistan',
            'country_iso_alpha2_code': 'TJ',
            'created_on': '2020-01-02 00:00:00',
            'datahub_company_id': '7b75ba30-88ae-4755-a096-fa842d18106c',
            'datahub_interaction_export_country_id': '7b8993d9-7ad7-4498-8e2c-f1913a14ea63',
            'datahub_interaction_id': '77c0d1a8-4fd4-4ef3-aac6-4d09cc45a710',
            'status': 'future_interest',
        },
        {
            'country_name': 'Saint Martin',
            'country_iso_alpha2_code': 'MF',
            'created_on': '2020-01-02 01:00:00',
            'datahub_company_id': 'a858d27a-16e0-41e5-b925-5d0dd2dd6b69',
            'datahub_interaction_export_country_id': '337d599f-823f-4327-84d6-5cddfe26d02f',
            'datahub_interaction_id': '7e183db1-94d5-4266-a871-aac823afeb63',
            'status': 'currently_exporting',
        },
    ]
    extractor = source_data_extraction.ExtractDatahubInteractionsExportCountry
    item_pk = 'datahub_interaction_export_country_id'

    source_data = {
        'headers': [
            'company_id',
            'country_name',
            'country_iso_alpha2_code',
            'created_on',
            'id',
            'interaction_id',
            'status',
        ],
        'values': [
            [
                '7b75ba30-88ae-4755-a096-fa842d18106c',
                'Tajikistan',
                'TJ',
                '2020-01-02 00:00:00',
                '7b8993d9-7ad7-4498-8e2c-f1913a14ea63',
                '77c0d1a8-4fd4-4ef3-aac6-4d09cc45a710',
                'future_interest',
            ],
            [
                'a858d27a-16e0-41e5-b925-5d0dd2dd6b69',
                'Saint Martin',
                'MF',
                '2020-01-02 01:00:00',
                '337d599f-823f-4327-84d6-5cddfe26d02f',
                '7e183db1-94d5-4266-a871-aac823afeb63',
                'currently_exporting',
            ],
        ],
        'next': None,
    }
    source_table_id_config_key = 'datahub_interactions_export_country_source_table_id'


class TestExtractDatahubOmis(SourceDataExtractBaseTestCase):
    __test__ = True
    dataset_id_config_key = 'datahub_omis_dataset_id'
    item_pk = 'datahub_omis_order_id'
    expected_data = [
        {
            'company_id': 'c31e4492-1f16-48a2-8c5e-8c0334d959a3',
            'market': 'CN',
            'created_date': '2018-01-01 00:00:00',
            'datahub_omis_order_id': 'e84de2c0-fe7a-41fc-ba1d-5885925ff3ca',
            'sector': 'Aerospace',
        },
        {
            'company_id': 'd0af8e52-ff34-4088-98e3-d2d22cd250ae',
            'market': 'DE',
            'created_date': '2018-01-02 00:00:00',
            'datahub_omis_order_id': 'a3cc5ef5-0ec0-491a-aa48-48656d66e662',
            'sector': 'Food',
        },
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
    extractor = source_data_extraction.ExtractDatahubOmis


class TestExtractExportWins(SourceDataExtractBaseTestCase):
    __test__ = True
    dataset_id_config_key = 'export_wins_dataset_id'
    item_pk = 'export_wins_id'
    expected_data = [
        {
            'export_wins_id': '23f66b0e-05be-40a5-9bf2-fa44dc7714a8',
            'sector': 'Aerospace',
            'company_name': 'Spaceship',
            'export_wins_company_id': '20302012',
            'contact_email_address': 'test@spaceship.com',
            'created_on': '2019-01-02 18:00:00',
            'country': 'ES',
            'date_won': '2019-01-02',
        },
        {
            'export_wins_id': 'f50d892d-388a-405b-9e30-16b9971ac0d4',
            'sector': 'Food',
            'company_name': 'Cake',
            'export_wins_company_id': '9292929',
            'contact_email_address': 'test@cake.com',
            'created_on': '2020-01-20 11:00:00',
            'country': 'IR',
            'date_won': '2018-07-02',
        },
    ]
    source_data = {
        'headers': [
            'id',
            'sector',
            'company_name',
            'cdms_reference',
            'customer_email_address',
            'created',
            'country',
            'date',
        ],
        'values': [
            [
                '23f66b0e-05be-40a5-9bf2-fa44dc7714a8',
                'Aerospace',
                'Spaceship',
                '20302012',
                'test@spaceship.com',
                '2019-01-02 18:00',
                'ES',
                '2019-01-02 18:00',
            ],
            [
                'f50d892d-388a-405b-9e30-16b9971ac0d4',
                'Food',
                'Cake',
                '9292929',
                'test@cake.com',
                '2020-01-20 11:00',
                'IR',
                '2018-07-02 10:00',
            ],
        ],
        'next': None,
    }
    source_table_id_config_key = 'export_wins_source_table_id'
    extractor = source_data_extraction.ExtractExportWins


class TestGetHawkHeaders:
    @patch('app.etl.tasks.source_data_extraction.mohawk')
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
        self.url = 'some_url'
        self.model = models.DatahubContact

    def get_rows(self):
        sql = f'select * from {self.model.__tablename__} order by id'
        status = flask_app.dbi.execute_statement(sql, raise_if_fail=True)
        return status.fetchall()

    def test_rollback_when_there_is_an_error(self, add_datahub_contact):
        add_datahub_contact(
            [
                {
                    'id': 1,
                    'datahub_company_id': '5e45d6d4-1fee-4065-9510-17fdaf63aff3',
                    'datahub_contact_id': '5ab0f5f2-a984-4252-ac2f-5a7a8037ad99',
                    'email': 'test1@test.com',
                },
                {
                    'id': 2,
                    'datahub_company_id': 'c3467472-3a97-4359-91f4-f860597e1837',
                    'datahub_contact_id': 'b99377a5-39f6-4474-8b84-dd96a6fd4832',
                    'email': 'test2@test.com',
                },
            ]
        )
        data = {
            'headers': ['id', 'company_id', 'email'],
            'values': [
                (
                    '5ab0f5f2-a984-4252-ac2f-5a7a8037ad99',
                    '5e45d6d4-1fee-4065-9510-17fdaf63aff3',
                    'test1@test.com',
                ),
                ('1', 'c3467472-3a97-4359-91f4-f860597e1837', 'test2@test.com'),
            ],
        }
        with pytest.raises(sqlalchemy.exc.DataError):
            source_data_extraction.populate_table(
                data,
                self.model,
                source_data_extraction.ExtractDatahubContactDataset.name,
                {'id': 'datahub_contact_id', 'company_id': 'datahub_company_id', 'email': 'email'},
                'datahub_contact_id',
                overwrite=False,
            )

        rows = self.get_rows()
        assert len(rows) == 2
        rows = self.get_rows()
        assert len(rows) == 2
        assert rows == [
            (
                1,
                uuid.UUID('5ab0f5f2-a984-4252-ac2f-5a7a8037ad99'),
                uuid.UUID('5e45d6d4-1fee-4065-9510-17fdaf63aff3'),
                'test1@test.com',
            ),
            (
                2,
                uuid.UUID('b99377a5-39f6-4474-8b84-dd96a6fd4832'),
                uuid.UUID('c3467472-3a97-4359-91f4-f860597e1837'),
                'test2@test.com',
            ),
        ]

    def test_if_overwrite_is_true_deletes_existing_data(self, add_datahub_contact):
        add_datahub_contact(
            [
                {
                    'id': 1,
                    'datahub_company_id': '5e45d6d4-1fee-4065-9510-17fdaf63aff3',
                    'datahub_contact_id': '5ab0f5f2-a984-4252-ac2f-5a7a8037ad99',
                    'email': 'test1@test.com',
                },
                {
                    'id': 2,
                    'datahub_company_id': 'c3467472-3a97-4359-91f4-f860597e1837',
                    'datahub_contact_id': 'b99377a5-39f6-4474-8b84-dd96a6fd4832',
                    'email': 'test2@test.com',
                },
            ]
        )
        data = {
            'headers': ['id', 'company_id', 'email'],
            'values': [
                (
                    '5ab0f5f2-a984-4252-ac2f-5a7a8037ad99',
                    '5e45d6d4-1fee-4065-9510-17fdaf63aff3',
                    'test1@test.com',
                ),
                (
                    'b99377a5-39f6-4474-8b84-dd96a6fd4832',
                    'c3467472-3a97-4359-91f4-f860597e1837',
                    'test2@test.com',
                ),
            ],
        }
        response = source_data_extraction.populate_table(
            data,
            self.model,
            source_data_extraction.ExtractDatahubContactDataset.name,
            {'id': 'datahub_contact_id', 'company_id': 'datahub_company_id', 'email': 'email'},
            'datahub_contact_id',
            overwrite=True,
        )
        assert response == {
            'rows': 2,
            'status': 200,
            'extractor': source_data_extraction.ExtractDatahubContactDataset.name,
        }

        rows = self.get_rows()
        assert len(rows) == 2
        assert rows == [
            (
                1,
                uuid.UUID('5ab0f5f2-a984-4252-ac2f-5a7a8037ad99'),
                uuid.UUID('5e45d6d4-1fee-4065-9510-17fdaf63aff3'),
                'test1@test.com',
            ),
            (
                2,
                uuid.UUID('b99377a5-39f6-4474-8b84-dd96a6fd4832'),
                uuid.UUID('c3467472-3a97-4359-91f4-f860597e1837'),
                'test2@test.com',
            ),
        ]

    def test_if_overwrite_is_false_upserts_to_table(self, add_datahub_contact):
        add_datahub_contact(
            [
                {
                    'id': 1,
                    'datahub_company_id': '5e45d6d4-1fee-4065-9510-17fdaf63aff3',
                    'datahub_contact_id': '5ab0f5f2-a984-4252-ac2f-5a7a8037ad99',
                    'email': 'test1@test.com',
                },
                {
                    'id': 2,
                    'datahub_company_id': 'c3467472-3a97-4359-91f4-f860597e1837',
                    'datahub_contact_id': 'b99377a5-39f6-4474-8b84-dd96a6fd4832',
                    'email': 'test2@test.com',
                },
            ]
        )
        data = {
            'headers': ['id', 'company_id', 'email'],
            'values': [
                (
                    '5ab0f5f2-a984-4252-ac2f-5a7a8037ad99',
                    '5e45d6d4-1fee-4065-9510-17fdaf63aff3',
                    'test1@test.com',
                ),
                (
                    '9703e296-cbcc-4b62-8ae9-68063cdd0275',
                    'c3467472-3a97-4359-91f4-f860597e1837',
                    'test2@test.com',
                ),
            ],
        }
        response = source_data_extraction.populate_table(
            data,
            self.model,
            source_data_extraction.ExtractDatahubContactDataset.name,
            {'id': 'datahub_contact_id', 'company_id': 'datahub_company_id', 'email': 'email'},
            'datahub_contact_id',
            overwrite=False,
        )
        assert response == {
            'rows': 2,
            'status': 200,
            'extractor': source_data_extraction.ExtractDatahubContactDataset.name,
        }

        rows = self.get_rows()
        assert len(rows) == 3
        assert rows == [
            (
                1,
                uuid.UUID('5ab0f5f2-a984-4252-ac2f-5a7a8037ad99'),
                uuid.UUID('5e45d6d4-1fee-4065-9510-17fdaf63aff3'),
                'test1@test.com',
            ),
            (
                2,
                uuid.UUID('b99377a5-39f6-4474-8b84-dd96a6fd4832'),
                uuid.UUID('c3467472-3a97-4359-91f4-f860597e1837'),
                'test2@test.com',
            ),
            (
                4,
                uuid.UUID('9703e296-cbcc-4b62-8ae9-68063cdd0275'),
                uuid.UUID('c3467472-3a97-4359-91f4-f860597e1837'),
                'test2@test.com',
            ),
        ]

    def test_if_overwrite_is_false_upserts_to_interaction_table(self, add_datahub_interaction):
        add_datahub_interaction(
            [
                {
                    'datahub_interaction_id': '5e45d6d4-1fee-4065-9510-17fdaf63aff3',
                    'datahub_company_id': 'a88ce197-552e-429e-9d47-da9853fdb6ba',
                    'subject': 'Subject 1',
                    'notes': 'Note 1',
                },
                {
                    'datahub_interaction_id': 'ff6f14c9-88a1-4c4d-b38b-eec8cb965f4b',
                    'datahub_company_id': 'c417614c-b6ac-4709-b52e-8de4ba83167e',
                    'subject': 'Subject 2',
                    'notes': 'Note 2',
                },
            ]
        )

        data = {
            'headers': [
                'datahub_id',
                'company_id',
                'interaction_subject',
                'interaction_notes',
                'created_on',
            ],
            'values': [
                (
                    'a6676fae-538c-4423-ba48-3f99a609b967',
                    '3905545d-c854-48ed-9340-8d2fc59eb8b2',
                    'New interaction',
                    'Interesting note',
                    None,
                ),
                (
                    '5e45d6d4-1fee-4065-9510-17fdaf63aff3',
                    'a88ce197-552e-429e-9d47-da9853fdb6ba',
                    'Subject 1',
                    'Note 1',
                    None,
                ),
                (
                    'ff6f14c9-88a1-4c4d-b38b-eec8cb965f4b',
                    'c417614c-b6ac-4709-b52e-8de4ba83167e',
                    'Updated Subject 2',
                    'Note 2',
                    None,
                ),
            ],
        }

        mapping = {
            'datahub_id': 'datahub_interaction_id',
            'company_id': 'datahub_company_id',
            'interaction_notes': 'notes',
            'interaction_subject': 'subject',
            'created_on': 'created_on',
        }
        response = source_data_extraction.populate_table(
            data,
            models.Interactions,
            'datahub_interactions',
            mapping,
            'datahub_interaction_id',
            overwrite=False,
        )
        assert response == {
            'rows': 3,
            'status': 200,
            'extractor': 'datahub_interactions',
        }

        sql = (
            f'select '
            f'id,datahub_interaction_id,datahub_company_id,subject,notes,created_on '
            f'from {models.Interactions.__tablename__} order by id'
        )
        status = flask_app.dbi.execute_statement(sql, raise_if_fail=True)
        rows = status.fetchall()
        assert len(rows) == 3
        assert rows == [
            (
                1,
                uuid.UUID('5e45d6d4-1fee-4065-9510-17fdaf63aff3'),
                uuid.UUID('a88ce197-552e-429e-9d47-da9853fdb6ba'),
                'Subject 1',
                'Note 1',
                None,
            ),
            (
                2,
                uuid.UUID('ff6f14c9-88a1-4c4d-b38b-eec8cb965f4b'),
                uuid.UUID('c417614c-b6ac-4709-b52e-8de4ba83167e'),
                'Updated Subject 2',
                'Note 2',
                None,
            ),
            (
                3,
                uuid.UUID('a6676fae-538c-4423-ba48-3f99a609b967'),
                uuid.UUID('3905545d-c854-48ed-9340-8d2fc59eb8b2'),
                'New interaction',
                'Interesting note',
                None,
            ),
        ]
