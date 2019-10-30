import mohawk, os, psycopg2
from etl.config import (
    datahub_company_primary_key,
    datahub_company_schema,
    datahub_company_table_name,
    datahub_export_countries_primary_key,
    datahub_export_countries_schema,
    datahub_export_countries_table_name,
    datahub_future_interest_countries_primary_key,
    datahub_future_interest_countries_schema,
    datahub_future_interest_countries_table_name,
    datahub_sector_primary_key,
    datahub_sector_schema,
    datahub_sector_table_name,
    omis_primary_key,
    omis_schema,
    omis_table_name,
)
from db import get_db
from utils import sql as sql_utils

dataworkspace_host = os.environ['DATAWORKSPACE_HOST']
dataworkspace_client_id = os.environ['DATAWORKSPACE_HAWK_CLIENT_ID']
dataworkspace_client_key = os.environ['DATAWORKSPACE_HAWK_CLIENT_KEY']
algorithm = 'sha256'
export_wins_host = os.environ['EXPORT_WINS_HOST']
export_wins_client_id = os.environ['EXPORT_WINS_HAWK_CLIENT_ID']
export_wins_client_key = os.environ['EXPORT_WINS_HAWK_CLIENT_KEY']

def extract_datahub_company_dataset():
    endpoint = '/api/v1/datahub-company-dataset'
    primary_key = datahub_company_primary_key
    schema = datahub_company_schema
    table_name = datahub_company_table_name
    url = 'http://{}/{}'.format(dataworkspace_host, endpoint)
    client_id = dataworkspace_client_id
    client_key = dataworkspace_client_key
    headers = get_hawk_headers(url, client_id, client_key)
    fields = ['company_number', 'id', 'sector']
    # TODO: remove stubbed data
    data = {
        'headers': ['company_number', 'id', 'sector'],
        'values': [
            ['1', 'c31e4492-1f16-48a2-8c5e-8c0334d959a3', 'Aerospace'],
            ['2', 'd0af8e52-ff34-4088-98e3-d2d22cd250ae', 'Food']
        ]
    }
    return populate_table(headers, schema, table_name, url, primary_key, stubbed_data=data)

def extract_datahub_export_countries():
    endpoint = '/api/v1/export-countries-dataset'
    primary_key = datahub_export_countries_primary_key
    schema = datahub_export_countries_schema
    table_name = datahub_export_countries_table_name
    url = 'http://{}/{}'.format(dataworkspace_host, endpoint)
    client_id = dataworkspace_client_id
    client_key = dataworkspace_client_key
    headers = get_hawk_headers(url, client_id, client_key)
    # TODO
    # data = requests.get(url, headers=headers).json()
    data = {
        'headers': ['company_id', 'id', 'country'],
        'values': [
            ['c31e4492-1f16-48a2-8c5e-8c0334d959a3', '3', 'US'],
            ['d0af8e52-ff34-4088-98e3-d2d22cd250ae', '2', 'MY']
        ]
    }
    return populate_table(headers, schema, table_name, url, primary_key, stubbed_data=data)

    
def extract_datahub_future_interest_countries():
    endpoint = '/api/v1/future-interest-countries-dataset'
    primary_key = datahub_future_interest_countries_primary_key
    schema = datahub_future_interest_countries_schema
    table_name = datahub_future_interest_countries_table_name
    url = 'http://{}/{}'.format(dataworkspace_host, endpoint)
    client_id = dataworkspace_client_id
    client_key = dataworkspace_client_key
    headers = get_hawk_headers(url, client_id, client_key)
    # TODO
    # data = requests.get(url, headers=headers).json()
    data = {
        'headers': ['company_id', 'id', 'country'],
        'values': [
            ['c31e4492-1f16-48a2-8c5e-8c0334d959a3', '1', 'CN'],
            ['d0af8e52-ff34-4088-98e3-d2d22cd250ae', '2', 'DE']
        ]
    }
    return populate_table(headers, schema, table_name, url, primary_key, stubbed_data=data)
    

def extract_datahub_omis_dataset():
    endpoint = '/api/v1/omis-dataset'
    primary_key = omis_primary_key
    schema = omis_schema
    table_name = omis_table_name
    url = 'http://{}/{}'.format(dataworkspace_host, endpoint)
    client_id = dataworkspace_client_id
    client_key = dataworkspace_client_key
    headers = get_hawk_headers(url, client_id, client_key)
    # TODO
    # data = requests.get(url, headers=headers).json()
    data = {
        'headers': ['company_id', 'country', 'created_on', 'id', 'sector'],
        'values': [
            [
                'c31e4492-1f16-48a2-8c5e-8c0334d959a3',
                'CN',
                '2018-01-01',
                'e84de2c0-fe7a-41fc-ba1d-5885925ff3ca',
                'Aerospace'
            ],
            [
                'd0af8e52-ff34-4088-98e3-d2d22cd250ae',
                'DE',
                '2018-01-02',
                'a3cc5ef5-0ec0-491a-aa48-48656d66e662',
                'Food'
            ]
        ]
    }
    return populate_table(headers, schema, table_name, url, primary_key, stubbed_data=data)

def extract_datahub_sectors():
    endpoint = '/api/v1/datahub-sectors-dataset'
    primary_key = datahub_sector_primary_key
    schema = datahub_sector_schema
    table_name = datahub_sector_table_name
    url = 'http://{}/{}'.format(dataworkspace_host, endpoint)
    client_id = dataworkspace_client_id
    client_key = dataworkspace_client_key
    headers = get_hawk_headers(url, client_id, client_key)
    # TODO
    # data = requests.get(url, headers=headers).json()
    data = {
        'headers': ['sector'],
        'values': ['Aerospace', 'Food']
    }
    return populate_table(headers, schema, table_name, url, primary_key, stubbed_data=data)

def extract_export_wins():
    endpoint = '/api/v1/datahub-sectors-dataset'
    primary_key = 'id'
    schema = (
        'id uuid',
        'company_id varchar(12)',
        'country varchar(2)',
        'timestamp timestamp'
    )
    table_name = 'export_wins'
    url = 'http://{}/{}'.format(export_wins_host, endpoint)
    client_id = export_wins_client_id
    client_key = export_wins_client_key
    headers = get_hawk_headers(url, export_wins_client_id, export_wins_client_key)
    # TODO, remove stubbed data
    data = {
        'headers': ['id', 'companyId', 'timestamp'],
        'values': [
            ['23f66b0e-05be-40a5-9bf2-fa44dc7714a8', 'asdf', 'IT', '2019-01-01 1:00'],
            ['f50d892d-388a-405b-9e30-16b9971ac0d4', 'ffff', 'GO', '2019-01-02 18:00']
        ]
    }
    return populate_table(headers, schema, table_name, url, primary_key, stubbed_data=data)
    

def get_hawk_headers(
        url,
        client_id,
        client_key,
        content='',
        content_type='',
        method='GET'
):

    credentials = {
        'id': client_id,
        'key': client_key,
        'algorithm': algorithm
    }
    
    sender = mohawk.Sender(
        credentials=credentials,
        url=url,
        method=method,
        content=content,
        content_type=content_type
    )
    headers = {
        'Authorization': sender.request_header,
        'Content-Type': content_type
    }
    return headers

def populate_table(headers, schema, table_name, url, primary_key=None, stubbed_data={}):

    if len(stubbed_data) > 0:
        data = stubbed_data
    else:
        reponse = requests.get(url, headers=headers)
        data = response.json()

    connection = get_db()
    cursor = connection.cursor()
    table_exists = sql_utils.table_exists(connection, table_name)

    try:
        if table_exists:
            table_name_backup = '{}_backup'.format(table_name)
            sql_utils.drop_table(connection, table_name_backup)
            sql_utils.rename_table(connection, table_name, table_name_backup)

        schema_str = ','.join(schema)
        if primary_key is not None:
            if type(primary_key) in [list, tuple]:
                schema_str += ', primary key ({})'.format(','.join(primary_key))
            else:
                schema_str += ', primary key ({})'.format(primary_key)
        sql = '''create table {table_name} ({schema_str})'''.format(
            table_name=table_name,
            schema_str=schema_str
        )
        cursor.execute(sql)
        connection.commit()
        
        sql = ''' insert into {} values ({}) '''.format(
            table_name,
            ','.join(['%s' for i in range(len(schema))])
        )
        print('sql:', sql)
        values = data['values'] if len(schema) > 1 else [[d] for d in data['values']]
        result = cursor.executemany(sql, values)
        connection.commit()
        n_rows = cursor.rowcount
        output = {
            'table': table_name,
            'rows': n_rows,
            'status': 'success'
        }
        print(output)

    except (Exception, psycopg2.Error) as error:
        output = "Failed inserting record into {} table {}".format(table_name, error)
        print(output)

        if table_exists:
            sql = ''' alter table {} rename to {} '''.format(table_name_backup, table_name)
            cursor.execute(sql)
            connection.commit()

    return output
    
