from flask import current_app

import mohawk

import requests

from app.db.models import sql_alchemy


class SourceDataExtractor:
    def __call__(self):
        if current_app.config['app']['stub_source_data']:
            return populate_table(self.stub_data, self.schema, self.table_name)
        else:
            url = self.get_url()
            return populate_table_paginated(self.schema, self.table_name, url)

    def get_url(self):
        dataworkspace_config = current_app.config['dataworkspace']
        dataset_id = dataworkspace_config[self.dataset_id_config_key]
        source_table_id = dataworkspace_config[self.source_table_id_config_key]
        endpoint = f'api/v1/dataset/{dataset_id}/{source_table_id}'
        url = f'{dataworkspace_config["base_url"]}/{endpoint}'
        return url


class ReferenceDatasetExtractor(SourceDataExtractor):
    def get_url(self):
        dataworkspace_config = current_app.config['dataworkspace']
        group_slug = dataworkspace_config[self.group_slug]
        reference_slug = dataworkspace_config[self.reference_slug]
        endpoint = f'api/v1/reference-dataset/{group_slug}/reference/{reference_slug}'
        url = f'{dataworkspace_config["base_url"]}/{endpoint}'
        return url


class ExtractCountriesAndTerritoriesReferenceDataset(ReferenceDatasetExtractor):

    group_slug = 'countries_and_territories_group_slug'
    reference_slug = 'countries_and_territories_reference_slug'
    schema = {
        'columns': [
            {'name': 'id', 'type': 'varchar(50)', 'source_name': 'ID'},
            {'name': 'name', 'type': 'varchar(200)', 'source_name': 'Name'},
            {'name': 'type', 'type': 'varchar(100)', 'source_name': 'Type'},
            {'name': 'start_date', 'type': 'date', 'source_name': 'Start Date'},
            {'name': 'end_date', 'type': 'date', 'source_name': 'End Date'},
        ],
        'primary_key': 'id',
    }
    stub_data = {
        'headers': ['ID', 'Name', 'Type', 'Start Date', 'End Date'],
        'values': [
            ['AE-AZ', 'Abu Dhabi', 'Territory', None, None],
            ['AF', 'Afghanistan', 'Country', None, None],
            ['AO', 'Angola', 'Country', '1975-11-11', None],
        ],
    }
    table_name = 'reference_countries_and_territories'


class ExtractDatahubCompanyDataset(SourceDataExtractor):
    dataset_id_config_key = 'datahub_companies_dataset_id'
    source_table_id_config_key = 'datahub_companies_source_table_id'
    schema = {
        'columns': [
            {'name': 'id', 'type': 'uuid'},
            {'name': 'company_number', 'type': 'varchar(50)'},
            {'name': 'sector', 'type': 'varchar(50)'},
        ],
        'primary_key': 'id',
    }
    stub_data = {
        'headers': ['id', 'company_number', 'sector'],
        'values': [
            ['c31e4492-1f16-48a2-8c5e-8c0334d959a3', 'asdf', 'Food'],
            ['d0af8e52-ff34-4088-98e3-d2d22cd250ae', 'asdf2', 'Aerospace'],
        ],
    }
    table_name = 'datahub_company'


class ExtractDatahubExportCountries(SourceDataExtractor):
    dataset_id_config_key = 'datahub_export_countries_dataset_id'
    source_table_id_config_key = 'datahub_export_countries_source_table_id'
    schema = {
        'columns': [
            {'name': 'company_id', 'type': 'uuid'},
            {'name': 'country_iso_alpha2_code', 'type': 'varchar(2)'},
            {'name': 'id', 'type': 'int'},
        ],
        'primary_key': 'id',
    }
    stub_data = {
        'headers': ['company_id', 'country_iso_alpha2_code', 'id'],
        'values': [
            ['c31e4492-1f16-48a2-8c5e-8c0334d959a3', 'US', 1],
            ['d0af8e52-ff34-4088-98e3-d2d22cd250ae', 'MY', 2],
        ],
    }
    table_name = 'datahub_export_countries'


class ExtractDatahubFutureInterestCountries(SourceDataExtractor):
    dataset_id_config_key = 'datahub_future_interest_countries_dataset_id'
    source_table_id_config_key = 'datahub_future_interest_countries_source_table_id'
    schema = {
        'columns': [
            {'name': 'company_id', 'type': 'uuid'},
            {'name': 'country_iso_alpha2_code', 'type': 'varchar(2)'},
            {'name': 'id', 'type': 'int'},
        ],
        'primary_key': 'id',
    }
    stub_data = {
        'headers': ['company_id', 'country_iso_alpha2_code', 'id'],
        'values': [
            ['c31e4492-1f16-48a2-8c5e-8c0334d959a3', 'CN', 1],
            ['d0af8e52-ff34-4088-98e3-d2d22cd250ae', 'DE', 2],
        ],
    }
    table_name = 'datahub_future_interest_countries'


class ExtractDatahubInteractions(SourceDataExtractor):
    dataset_id_config_key = 'datahub_interactions_dataset_id'
    source_table_id_config_key = 'datahub_interactions_source_table_id'
    schema = {
        'columns': [
            {'name': 'id', 'type': 'uuid'},
            {'name': 'event_id', 'type': 'uuid'},
            {'name': 'company_id', 'type': 'uuid'},
            {'name': 'created_on', 'type': 'timestamp'},
            {'name': 'interaction_date', 'type': 'timestamp'},
        ],
        'primary_key': 'id',
    }
    stub_data = {
        'headers': ['id', 'event_id', 'company_id', 'created_on', 'interaction_date'],
        'values': [
            [
                'a8cb910f-51df-4d8e-a953-01c0be435d36',
                '7cd493ec-8e1c-4bbc-a0ba-ebd8fd118381',
                '05b2acd6-21cb-4a98-a857-d5ff773db4ff',
                '2019-01-01 01:00:00',
                '2019-01-01',
            ],
            [
                'c31e4492-1f16-48a2-8c5e-8c0334d959a3',
                '0774cc83-11e7-4100-8631-3b8b0998c514',
                '8ef278b1-0bde-4f25-8279-36f9ba05198d',
                '2019-01-02 02:00:00',
                '2019-01-02',
            ],
        ],
    }
    table_name = 'datahub_interactions'


class ExtractDatahubOmis(SourceDataExtractor):
    dataset_id_config_key = 'datahub_omis_dataset_id'
    source_table_id_config_key = 'datahub_omis_source_table_id'
    schema = {
        'columns': [
            {'name': 'company_id', 'type': 'uuid'},
            {'name': 'market', 'type': 'varchar(2)'},
            {'name': 'created_date', 'type': 'timestamp'},
            {'name': 'id', 'type': 'uuid'},
            {'name': 'sector', 'type': 'varchar(200)'},
        ],
        'primary_key': 'id',
    }
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
    table_name = 'datahub_omis'


class ExtractDatahubSectors(SourceDataExtractor):
    dataset_id_config_key = 'datahub_sectors_dataset_id'
    source_table_id_config_key = 'datahub_sectors_source_table_id'
    schema = {
        'columns': [
            {'name': 'id', 'type': 'uuid'},
            {'name': 'sector', 'type': 'varchar(200)'},
        ],
        'primary_key': 'id',
    }
    stub_data = {
        'headers': ['id', 'sector'],
        'values': [
            ['c3467472-3a97-4359-91f4-f860597e1837', 'Aerospace'],
            ['698d0cc3-ce8e-453b-b3c4-99818c5a9070', 'Food'],
        ],
    }
    table_name = 'datahub_sectors'


class ExtractExportWins(SourceDataExtractor):
    dataset_id_config_key = 'export_wins_dataset_id'
    source_table_id_config_key = 'export_wins_source_table_id'
    schema = {
        'columns': [
            {'name': 'id', 'type': 'uuid'},
            {'name': 'company_id', 'type': 'varchar(12)'},
            {'name': 'country', 'type': 'varchar(2)'},
            {'name': 'timestamp', 'type': ' timestamp'},
        ],
        'primary_key': 'id',
    }
    stub_data = {
        'headers': ['id', 'company_id', 'country', 'timestamp'],
        'values': [
            ['23f66b0e-05be-40a5-9bf2-fa44dc7714a8', 'asdf', 'IT', '2019-01-01 1:00'],
            ['f50d892d-388a-405b-9e30-16b9971ac0d4', 'ffff', 'GO', '2019-01-02 18:00'],
        ],
    }
    table_name = 'export_wins'


def get_hawk_headers(
    url, client_id, client_key, content='', content_type='', https=False, method='GET',
):

    if https is False:
        url = url.replace('https', 'http')
    credentials = {'id': client_id, 'key': client_key, 'algorithm': 'sha256'}

    sender = mohawk.Sender(
        credentials=credentials,
        url=url,
        method=method,
        content=content,
        content_type=content_type,
    )
    headers = {'Authorization': sender.request_header, 'Content-Type': content_type}
    return headers


def populate_table_paginated(schema, table_name, url):
    client_id = current_app.config['dataworkspace']['hawk_client_id']
    client_key = current_app.config['dataworkspace']['hawk_client_key']
    headers = get_hawk_headers(url, client_id, client_key)

    next_page = url
    n_rows = 0
    while next_page is not None:
        response = requests.get(next_page, headers=headers)
        data = response.json()
        output = populate_table(data, schema, table_name, overwrite=next_page == url)
        n_rows = n_rows + output['rows']
        next_page = data['next']
    return {'table': table_name, 'rows': n_rows, 'status': 200}


def populate_table(data, schema, table_name, overwrite=True):
    connection = sql_alchemy.engine.connect()
    transaction = connection.begin()

    if overwrite is True:
        # drop table
        sql = 'drop table if exists {}'.format(table_name)
        connection.execute(sql)

    # create table
    schema_str = ','.join(
        map(
            lambda column: "{} {}".format(column['name'], column['type']),
            schema['columns'],
        ),
    )
    primary_key = schema.get('primary_key')
    if primary_key is not None:
        if type(primary_key) in [list, tuple]:
            schema_str += ', primary key ({})'.format(','.join(primary_key))
        else:
            schema_str += ', primary key ({})'.format(primary_key)
    sql = 'create table if not exists {} ({})'.format(table_name, schema_str)
    connection.execute(sql)

    # insert into table
    column_names_destination = [column['name'] for column in schema['columns']]
    column_names_source = [
        column.get('source_name', column['name']) for column in schema['columns']
    ]
    sql = '''
    insert into {} ({}) values ({}) on conflict ({}) do update set {}
    '''.format(
        table_name,
        ','.join(column_names_destination),
        ','.join(['%s' for i in column_names_destination]),
        (
            schema['primary_key']
            if type(schema['primary_key']) == str
            else ','.join(schema['primary_key'])
        ),
        ','.join(['{}=%s'.format(c) for c in column_names_destination]),
    )
    column_indices = list(map(data['headers'].index, list(column_names_source)))
    values = list(map(lambda row: 2 * [row[i] for i in column_indices], data['values']))
    if len(values):
        status = connection.execute(sql, values)
        n_rows = int(status.rowcount)
    else:
        n_rows = 0
    transaction.commit()
    connection.close()

    return {'table': table_name, 'rows': n_rows, 'status': 200}


extract_countries_and_territories_reference_dataset = (
    ExtractCountriesAndTerritoriesReferenceDataset()
)
extract_datahub_company_dataset = ExtractDatahubCompanyDataset()
extract_datahub_export_countries = ExtractDatahubExportCountries()
extract_datahub_future_interest_countries = ExtractDatahubFutureInterestCountries()
extract_datahub_interactions = ExtractDatahubInteractions()
extract_datahub_omis = ExtractDatahubOmis()
extract_datahub_sectors = ExtractDatahubSectors()
extract_export_wins = ExtractExportWins()
