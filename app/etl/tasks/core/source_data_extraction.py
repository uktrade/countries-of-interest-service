from flask import current_app

import mohawk

import requests

from app.db.models import sql_alchemy

import utils


class SourceDataExtractor:
    def __call__(self):
        if current_app.config['app']['stub_source_data']:
            return populate_table(self.stub_data, self.schema, self.table_name)
        else:
            dataworkspace_config = current_app.config['dataworkspace']
            dataset_id = dataworkspace_config[self.dataset_id_config_key]
            source_table_id = dataworkspace_config[self.source_table_id_config_key]
            endpoint = f'api/v1/dataset/{dataset_id}/{source_table_id}'
            url = f'http://{dataworkspace_config["host"]}/{endpoint}'
            return populate_table_paginated(self.schema, self.table_name, url)


class ExtractDatahubCompanyDataset(SourceDataExtractor):
    dataset_id_config_key = 'datahub_company_dataset_id'
    source_table_id_config_key = 'datahub_company_source_table_id'
    schema = {
        'columns': [
            {'name': 'id', 'type': 'uuid'},
            {'name': 'company_number', 'type': 'varchar(50)'},
            {'name': 'sector', 'type': 'varchar(50)'},
        ],
        'primary_key': 'id',
    }
    stub_data = {
        'headers': ['id', 'companyNumber', 'sector'],
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
            {'name': 'country', 'type': 'varchar(2)'},
            {'name': 'id', 'type': 'int'},
        ],
        'primary_key': 'id',
    }
    stub_data = {
        'headers': ['companyId', 'country', 'id'],
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
            {'name': 'country', 'type': 'varchar(2)'},
            {'name': 'id', 'type': 'int'},
        ],
        'primary_key': 'id',
    }
    stub_data = {
        'headers': ['companyId', 'country', 'id'],
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
            {'name': 'company_id', 'type': 'uuid'},
            {'name': 'country', 'type': 'varchar(2)'},
            {'name': 'id', 'type': 'int'},
        ],
        'primary_key': 'id',
    }
    stub_data = {
        'headers': ['companyId', 'country', 'id'],
        'values': [
            ['c31e4492-1f16-48a2-8c5e-8c0334d959a3', 'CN', 1],
            ['d0af8e52-ff34-4088-98e3-d2d22cd250ae', 'DE', 2],
        ],
    }
    table_name = 'datahub_interactions'


class ExtractDatahubOmis(SourceDataExtractor):
    dataset_id_config_key = 'datahub_omis_dataset_id'
    source_table_id_config_key = 'datahub_omis_source_table_id'
    schema = {
        'columns': [
            {'name': 'company_id', 'type': 'uuid'},
            {'name': 'country', 'type': 'varchar(2)'},
            {'name': 'created_on', 'type': 'timestamp'},
            {'name': 'id', 'type': 'uuid'},
            {'name': 'sector', 'type': 'varchar(200)'},
        ],
        'primary_key': 'id',
    }
    stub_data = {
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
        'headers': ['id', 'companyId', 'country', 'timestamp'],
        'values': [
            ['23f66b0e-05be-40a5-9bf2-fa44dc7714a8', 'asdf', 'IT', '2019-01-01 1:00'],
            ['f50d892d-388a-405b-9e30-16b9971ac0d4', 'ffff', 'GO', '2019-01-02 18:00'],
        ],
    }
    table_name = 'export_wins'


def get_hawk_headers(
    url, client_id, client_key, content='', content_type='', method='GET'
):

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
        print('\033[33m drop table \033[0m')
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
    column_names = [column['name'] for column in schema['columns']]
    sql = '''
    insert into {} ({}) values ({}) on conflict ({}) do update set {}
    '''.format(
        table_name,
        ','.join(column_names),
        ','.join(['%s' for i in column_names]),
        (
            schema['primary_key']
            if type(schema['primary_key']) == str
            else ','.join(schema['primary_key'])
        ),
        ','.join(['{}=%s'.format(c) for c in column_names]),
    )
    column_names_camel = list(map(utils.to_camel_case, column_names))
    column_indices = list(map(data['headers'].index, list(column_names_camel)))
    values = list(map(lambda row: 2 * [row[i] for i in column_indices], data['values']))
    status = connection.execute(sql, values)
    n_rows = int(status.rowcount)
    transaction.commit()
    connection.close()

    return {'table': table_name, 'rows': n_rows, 'status': 200}


extract_datahub_company_dataset = ExtractDatahubCompanyDataset()
extract_datahub_export_countries = ExtractDatahubExportCountries()
extract_datahub_future_interest_countries = ExtractDatahubFutureInterestCountries()
extract_datahub_interactions = ExtractDatahubInteractions()
extract_datahub_omis = ExtractDatahubOmis()
extract_datahub_sectors = ExtractDatahubSectors()
extract_export_wins = ExtractExportWins()
