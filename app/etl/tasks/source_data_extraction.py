import mohawk
import requests
from flask import current_app as flask_app
from sqlalchemy import exc
from sqlalchemy.dialects import postgresql

import app.db.models.external as models
from app.config.constants import Source
from app.db.models import sql_alchemy


class SourceDataExtractor:
    stub_data = {}
    model = None
    mapping = {}
    unique_key = 'id'
    name = None

    def __call__(self):
        if flask_app.config['app']['stub_source_data']:
            return populate_table(
                self.stub_data, self.model, self.name, self.mapping, self.unique_key
            )
        else:
            url = self.get_url()
            return populate_table_paginated(
                self.model, self.name, self.mapping, self.unique_key, url
            )

    def get_url(self):
        dataworkspace_config = flask_app.config['dataworkspace']
        dataset_id = dataworkspace_config[self.dataset_id_config_key]
        source_table_id = dataworkspace_config[self.source_table_id_config_key]
        endpoint = f'api/v1/dataset/{dataset_id}/{source_table_id}'
        url = f'{dataworkspace_config["base_url"]}/{endpoint}'
        return url


class ReferenceDatasetExtractor(SourceDataExtractor):
    def get_url(self):
        dataworkspace_config = flask_app.config['dataworkspace']
        group_slug = dataworkspace_config[self.group_slug]
        reference_slug = dataworkspace_config[self.reference_slug]
        endpoint = f'api/v1/reference-dataset/{group_slug}/reference/{reference_slug}'
        url = f'{dataworkspace_config["base_url"]}/{endpoint}'
        return url


class ExtractCountriesAndTerritoriesReferenceDataset(ReferenceDatasetExtractor):
    name = Source.COUNTRIES_AND_TERRITORIES.value
    group_slug = 'countries_and_territories_group_slug'
    mapping = {
        'ID': 'country_iso_alpha2_code',
        'Name': 'name',
        'Type': 'type',
        'Start date': 'start_date',
        'End date': 'end_date',
    }
    model = models.DITCountryTerritoryRegister
    reference_slug = 'countries_and_territories_reference_slug'
    stub_data = {
        'headers': ['ID', 'Name', 'Type', 'Start date', 'End date'],
        'values': [
            ['AE-AZ', 'Abu Dhabi', 'Territory', None, None],
            ['AF', 'Afghanistan', 'Country', None, None],
            ['AO', 'Angola', 'Country', '1975-11-11', None],
            ['CN', 'China', 'Country', None, None],
            ['DE', 'Germany', 'Country', None, None],
            ['MY', 'Myanmar', 'Country', None, None],
            ['US', 'United States', 'Country', None, None],
        ],
    }
    unique_key = 'country_iso_alpha2_code'


class ExtractDatahubCompanyDataset(SourceDataExtractor):
    name = Source.DATAHUB_COMPANY.value
    dataset_id_config_key = 'datahub_companies_dataset_id'
    mapping = {
        'id': 'datahub_company_id',
        'name': 'company_name',
        'company_number': 'companies_house_id',
        'sector': 'sector',
        'cdms_reference_code': 'reference_code',
        'address_postcode': 'postcode',
        'modified_on': 'modified_on',
    }
    model = models.DatahubCompany
    source_table_id_config_key = 'datahub_companies_source_table_id'
    stub_data = {
        'headers': [
            'id',
            'name',
            'company_number',
            'sector',
            'reference_code',
            'address_postcode',
            'modified_on',
        ],
        'values': [
            [
                'c31e4492-1f16-48a2-8c5e-8c0334d959a3',
                'company 1',
                '01298764',
                'Food',
                'ORG-127698',
                'PE12 8KL',
                '2019-08-20 00:00:00',
            ],
            [
                'd0af8e52-ff34-4088-98e3-d2d22cd250ae',
                'company 2',
                '19089376',
                'Aerospace',
                'ORG-18778',
                '123457',
                '2019-08-21 00:00:00',
            ],
        ],
    }
    unique_key = 'datahub_company_id'


class ExtractDatahubContactDataset(SourceDataExtractor):
    name = Source.DATAHUB_CONTACT.value
    dataset_id_config_key = 'datahub_contacts_dataset_id'
    mapping = {
        'id': 'datahub_contact_id',
        'company_id': 'datahub_company_id',
        'email': 'email',
    }
    model = models.DatahubContact
    source_table_id_config_key = 'datahub_contacts_source_table_id'
    stub_data = {
        'headers': ['id', 'company_id', 'email'],
        'values': [
            [
                'c31e4492-1f16-48a2-8c5e-8c0334d959a3',
                '6294212e-f863-44cc-b98c-6041384d6d56',
                'test@test.com',
            ],
            [
                'd0af8e52-ff34-4088-98e3-d2d22cd250ae',
                'a86018c3-e811-4472-af0f-125d36e858a6',
                'john@test.com',
            ],
        ],
    }
    unique_key = 'datahub_contact_id'


class ExtractDatahubExportCountryHistory(SourceDataExtractor):
    name = Source.DATAHUB_EXPORT_COUNTRY_HISTORY.value
    dataset_id_config_key = 'datahub_export_country_history_dataset_id'
    mapping = {
        'company_id': 'company_id',
        'country': 'country',
        'country_iso_alpha2_code': 'country_iso_alpha2_code',
        'history_date': 'history_date',
        'history_id': 'history_id',
        'history_type': 'history_type',
        'status': 'status',
    }
    model = models.DatahubExportCountryHistory
    source_table_id_config_key = 'datahub_export_country_history_source_table_id'
    stub_data = {
        'headers': [
            'company_id',
            'country_iso_alpha2_code',
            'country',
            'history_date',
            'history_id',
            'history_type',
            'status',
        ],
        'values': [
            [
                'c31e4492-1f16-48a2-8c5e-8c0334d959a3',
                'US',
                'united states',
                '2020-01-01 01:00',
                '897c05c7-7836-4d36-b44d-56d1c3ae6a9a',
                'insert',
                'future_interest',
            ],
            [
                'd0af8e52-ff34-4088-98e3-d2d22cd250ae',
                'MY',
                'myanmar',
                '2020-01-01 02:00',
                '450879d1-6169-4e32-965f-47474640f3ae',
                'delete',
                'not_interested',
            ],
        ],
    }


class ExtractDatahubExportToCountries(SourceDataExtractor):
    name = Source.DATAHUB_EXPORT_TO_COUNTRIES.value
    dataset_id_config_key = 'datahub_export_countries_dataset_id'
    mapping = {
        'company_id': 'company_id',
        'country_iso_alpha2_code': 'country_iso_alpha2_code',
        'country': 'country',
        'id': 'id',
    }
    model = models.DatahubExportToCountries
    source_table_id_config_key = 'datahub_export_countries_source_table_id'
    stub_data = {
        'headers': ['company_id', 'country_iso_alpha2_code', 'country', 'id'],
        'values': [
            ['c31e4492-1f16-48a2-8c5e-8c0334d959a3', 'US', 'united states', 1],
            ['d0af8e52-ff34-4088-98e3-d2d22cd250ae', 'MY', 'myanmar', 2],
        ],
    }


class ExtractDatahubFutureInterestCountries(SourceDataExtractor):
    name = Source.DATAHUB_FUTURE_INTEREST_COUNTRIES.value
    dataset_id_config_key = 'datahub_future_interest_countries_dataset_id'
    mapping = {
        'company_id': 'company_id',
        'country_iso_alpha2_code': 'country_iso_alpha2_code',
        'country': 'country',
        'id': 'id',
    }
    model = models.DatahubFutureInterestCountries
    source_table_id_config_key = 'datahub_future_interest_countries_source_table_id'
    stub_data = {
        'headers': ['company_id', 'country_iso_alpha2_code', 'country', 'id'],
        'values': [
            ['c31e4492-1f16-48a2-8c5e-8c0334d959a3', 'CN', 'china', 1],
            ['d0af8e52-ff34-4088-98e3-d2d22cd250ae', 'DE', 'germany', 2],
        ],
    }


class ExtractDatahubInteractions(SourceDataExtractor):
    name = Source.DATAHUB_INTERACTIONS.value
    dataset_id_config_key = 'datahub_interactions_dataset_id'
    mapping = {
        'id': 'datahub_interaction_id',
        'company_id': 'datahub_company_id',
        'interaction_notes': 'notes',
        'interaction_subject': 'subject',
        'created_on': 'created_on',
    }
    model = models.Interactions
    source_table_id_config_key = 'datahub_interactions_source_table_id'
    stub_data = {
        'headers': ['id', 'company_id', 'interaction_notes', 'interaction_subject', 'created_on'],
        'values': [
            [
                'a8cb910f-51df-4d8e-a953-01c0be435d36',
                '7cd493ec-8e1c-4bbc-a0ba-ebd8fd118381',
                'A meeting with Apple',
                'Project X',
                '2019-01-01',
            ],
            [
                'c31e4492-1f16-48a2-8c5e-8c0334d959a3',
                '0774cc83-11e7-4100-8631-3b8b0998c514',
                'Lunch with bob in LA',
                'Phone',
                '2019-01-02',
            ],
        ],
    }
    unique_key = 'datahub_interaction_id'


class ExtractDatahubOmis(SourceDataExtractor):
    name = Source.DATAHUB_OMIS.value
    dataset_id_config_key = 'datahub_omis_dataset_id'
    mapping = {
        'company_id': 'company_id',
        'market': 'market',
        'created_date': 'created_date',
        'id': 'datahub_omis_order_id',
        'sector': 'sector',
    }
    model = models.DatahubOmis
    source_table_id_config_key = 'datahub_omis_source_table_id'
    stub_data = {
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
    }
    unique_key = 'datahub_omis_order_id'


class ExtractExportWins(SourceDataExtractor):
    name = Source.EXPORT_WINS.value
    dataset_id_config_key = 'export_wins_dataset_id'
    mapping = {
        'id': 'export_wins_id',
        'sector': 'sector',
        'company_name': 'company_name',
        'cdms_reference': 'export_wins_company_id',
        'customer_email_address': 'contact_email_address',
        'created': 'created_on',
        'country': 'country',
        'date': 'date_won',
    }
    model = models.ExportWins
    source_table_id_config_key = 'export_wins_source_table_id'
    stub_data = {
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
    }
    unique_key = 'export_wins_id'


def get_hawk_headers(
    url, client_id, client_key, content='', content_type='', https=False, method='GET',
):
    if https is False:
        # todo: ask data workspace to fix the https/http x-forwarded-for
        url = url.replace('https', 'http')
    # strip query string
    url = url.split('?')[0]
    credentials = {'id': client_id, 'key': client_key, 'algorithm': 'sha256'}

    sender = mohawk.Sender(
        credentials=credentials, url=url, method=method, content=content, content_type=content_type,
    )
    headers = {'Authorization': sender.request_header, 'Content-Type': content_type}
    return headers


def populate_table_paginated(model, extractor_name, mapping, unique_key, url):
    client_id = flask_app.config['dataworkspace']['hawk_client_id']
    client_key = flask_app.config['dataworkspace']['hawk_client_key']
    next_page = url
    n_rows = 0
    while next_page is not None:
        headers = get_hawk_headers(next_page, client_id, client_key)
        response = requests.get(next_page, headers=headers)
        data = response.json()
        output = populate_table(
            data, model, extractor_name, mapping, unique_key, overwrite=next_page == url
        )
        n_rows = n_rows + output['rows']
        next_page = data['next']
    return {'extractor': extractor_name, 'rows': n_rows, 'status': 200}


def populate_table(data, model, extractor_name, mapping, unique_key, overwrite=True):
    connection = sql_alchemy.engine.connect()
    transaction = connection.begin()
    try:
        if overwrite:
            model.recreate_table()
        items = []
        for item in data['values']:
            data_item = dict(zip(data['headers'], item))
            data_item = map_headers(data_item, mapping)
            items.append(data_item)

        insert_stmt = postgresql.insert(model.__table__).values(items)
        update_columns = {col.name: col for col in insert_stmt.excluded if col.name not in ('id',)}
        update_statement = insert_stmt.on_conflict_do_update(
            index_elements=[unique_key], set_=update_columns
        )
        status = connection.execute(update_statement)
        n_rows = int(status.rowcount)
        transaction.commit()
    except (exc.ProgrammingError, exc.DataError) as err:
        flask_app.logger.error(f'Error populating {model.__tablename__} table')
        flask_app.logger.error(err)
        transaction.rollback()
        raise err
    finally:
        connection.close()

    return {'extractor': extractor_name, 'rows': n_rows, 'status': 200}


def map_headers(data_item, db_headers):
    mapped_item = {}
    for k, v in data_item.items():
        header = db_headers.get(k)
        if header:
            mapped_item[header] = v
    return mapped_item
