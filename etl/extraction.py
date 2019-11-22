import mohawk, os, psycopg2, requests
from flask import current_app
from db import get_db
from utils import sql as sql_utils


def extract_datahub_company_dataset():
    endpoint = 'api/v1/dataset/datahub-company-dataset'
    table_name = 'datahub_company'
    schema = {
        'columns': ('id uuid', 'company_number varchar(50)', 'sector varchar(50)'),
        'primary_key': 'id',
    }
    url = 'http://{}/{}'.format(current_app.config['DATAWORKSPACE_HOST'], endpoint)
    # TODO: remove stubbed data
    data = {
        'headers': ['id', 'companyNumber', 'sector'],
        'values': [
            ['c31e4492-1f16-48a2-8c5e-8c0334d959a3', 'asdf', 'Food'],
            ['d0af8e52-ff34-4088-98e3-d2d22cd250ae', 'asdf2', 'Aerospace'],
        ]
    }
    return populate_table(schema, table_name, url, stub_data=data)

def extract_datahub_export_countries():
    endpoint = 'api/v1/dataset/datahub-export-countries'
    schema = {
        'columns': ('company_id uuid', 'country varchar(2)', 'id int'),
        'primary_key': 'id',
    }
    table_name = 'datahub_export_countries'
    url = 'http://{}/{}'.format(current_app.config['DATAWORKSPACE_HOST'], endpoint)
    data = {
        'headers': ['company_id', 'country', 'id', ],
        'values': [
            ['c31e4492-1f16-48a2-8c5e-8c0334d959a3', 'US', 1],
            ['d0af8e52-ff34-4088-98e3-d2d22cd250ae', 'MY', 2]
        ]
    }
    return populate_table(schema, table_name, url, stub_data=data)

    
def extract_datahub_future_interest_countries():
    endpoint = 'api/v1/dataset/datahub-future-interest-countries'
    schema = {
        'columns': (
            'company_id uuid',
            'country varchar(2)',
            'id int',
        ),
        'primary_key': 'id',
    }
    table_name = 'datahub_future_interest_countries'
    url = 'http://{}/{}'.format(current_app.config['DATAWORKSPACE_HOST'], endpoint)
    data = {
        'headers': ['companyId', 'country', 'id', ],
        'values': [
            ['c31e4492-1f16-48a2-8c5e-8c0334d959a3', 'CN', 1],
            ['d0af8e52-ff34-4088-98e3-d2d22cd250ae', 'DE', 2]
        ]
    }
    return populate_table(schema, table_name, url, stub_data=data)


def extract_datahub_interactions():
    endpoint = 'api/v1/dataset/datahub-interactions'
    # todo what does this schema look like
    schema = {
        'columns': (
            'company_id uuid',
            'country varchar(2)',
            'id int',
        ),
        'primary_key': 'id',
    }
    table_name = 'datahub_interactions'
    url = 'http://{}/{}'.format(current_app.config['DATAWORKSPACE_HOST'], endpoint)
    stub_data = {
        'headers': ['companyId', 'country', 'id', ],
        'values': [
            ['c31e4492-1f16-48a2-8c5e-8c0334d959a3', 'CN', 1],
            ['d0af8e52-ff34-4088-98e3-d2d22cd250ae', 'DE', 2]
        ]
    }
    return populate_table(schema, table_name, url, stub_data=stub_data)


def extract_datahub_omis_dataset():
    endpoint = 'api/v1/omis-dataset'
    schema = {
        'columns': (
            'company_id uuid',
            'country varchar(2)',
            'created_on timestamp',
            'id uuid',
            'sector varchar(200)',
        ),
        'primary_key': 'id'
    }
    table_name = 'omis'
    url = 'http://{}/{}'.format(current_app.config['DATAWORKSPACE_HOST'], endpoint)
    data = {
        'headers': ['companyId', 'country', 'createdOn', 'id', 'sector'],
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
    return populate_table(schema, table_name, url, stub_data=data)

def extract_datahub_sectors():
    endpoint = 'api/v1/datahub-sectors-dataset'
    schema = {
        'columns': ('id uuid', 'sector varchar(200)'),
        'primary_key': 'id'
    }
    table_name = 'datahub_sector'
    url = 'http://{}/{}'.format(current_app.config['DATAWORKSPACE_HOST'], endpoint)
    data = {
        'headers': ['id', 'sector'],
        'values': [
            ['c3467472-3a97-4359-91f4-f860597e1837', 'Aerospace'],
            ['698d0cc3-ce8e-453b-b3c4-99818c5a9070', 'Food'],
        ]
    }
    return populate_table(schema, table_name, url, stub_data=data)

def extract_export_wins():
    endpoint = 'api/v1/export-wins'
    schema = {
        'columns': (
            'id uuid',
            'company_id varchar(12)',
            'country varchar(2)',
            'timestamp timestamp'
        ),
        'primary_key': 'id'
    }
    table_name = 'export_wins'
    dataworkspace_host = current_app.config['DATAWORKSPACE_HOST']
    url = 'http://{}/{}'.format(dataworkspace_host, endpoint)
    data = {
        'headers': ['id', 'companyId', 'timestamp'],
        'values': [
            ['23f66b0e-05be-40a5-9bf2-fa44dc7714a8', 'asdf', 'IT', '2019-01-01 1:00'],
            ['f50d892d-388a-405b-9e30-16b9971ac0d4', 'ffff', 'GO', '2019-01-02 18:00']
        ]
    }

    return populate_table(schema, table_name, url, stub_data=data)
    

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
        'algorithm': 'sha256'
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

def populate_table(schema, table_name, url, stub_data=None):
    
    if stub_data != None:
        data = stub_data
    else:
        client_id = current_app.config['DATAWORKSPACE_HAWK_CLIENT_ID']
        client_key = current_app.config['DATAWORKSPACE_HAWK_CLIENT_KEY']
        headers = get_hawk_headers(url, client_id, client_key)
        response = requests.get(url, headers=headers)
        data = response.json()

    connection = get_db()
    cursor = connection.cursor()
    table_exists = sql_utils.table_exists(connection, table_name)

    try:
        if table_exists:
            table_name_backup = '{}_backup'.format(table_name)
            sql_utils.drop_table(connection, table_name_backup)
            sql_utils.rename_table(connection, table_name, table_name_backup)

        schema_str = ','.join(schema['columns'])
        primary_key = schema.get('primary_key')
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
            ','.join(['%s' for i in range(len(schema['columns']))])
        )
        values = data['values'] if len(schema['columns']) > 1 else [[d] for d in data['values']]
        result = cursor.executemany(sql, values)
        connection.commit()
        n_rows = int(cursor.rowcount)
        output = {
            'table': table_name,
            'rows': n_rows,
            'status': 200
        }

    except (Exception, psycopg2.Error) as error:
        output = {
            'status': 500,
            'error': "Failed inserting record into {} table {}".format(table_name, error),
        }
        if table_exists:
            sql_utils.rename_table(connection, table_name_backup, table_name)

    return output
    

# def populate_table(headers, schema, table_name, url, primary_key=None, stubbed_data={}):
    
#     if len(stubbed_data) > 0:
#         data = stubbed_data
#     else:
#         reponse = requests.get(url, headers=headers)
#         data = response.json()

#     connection = get_db()
#     cursor = connection.cursor()
#     table_exists = sql_utils.table_exists(connection, table_name)

#     try:
#         if table_exists:
#             table_name_backup = '{}_backup'.format(table_name)
#             sql_utils.drop_table(connection, table_name_backup)
#             sql_utils.rename_table(connection, table_name, table_name_backup)

#         schema_str = ','.join(schema)
#         if primary_key is not None:
#             if type(primary_key) in [list, tuple]:
#                 schema_str += ', primary key ({})'.format(','.join(primary_key))
#             else:
#                 schema_str += ', primary key ({})'.format(primary_key)
#         sql = '''create table {table_name} ({schema_str})'''.format(
#             table_name=table_name,
#             schema_str=schema_str
#         )
#         cursor.execute(sql)
#         connection.commit()
        
#         sql = ''' insert into {} values ({}) '''.format(
#             table_name,
#             ','.join(['%s' for i in range(len(schema))])
#         )
#         values = data['values'] if len(schema) > 1 else [[d] for d in data['values']]
#         result = cursor.executemany(sql, values)
#         connection.commit()
#         n_rows = cursor.rowcount
#         output = {
#             'table': table_name,
#             'rows': n_rows,
#             'status': 'success'
#         }
#         print(output)

#     except (Exception, psycopg2.Error) as error:
#         output = "Failed inserting record into {} table {}".format(table_name, error)
#         print(output)

#         if table_exists:
#             sql = ''' alter table {} rename to {} '''.format(table_name_backup, table_name)
#             cursor.execute(sql)
#             connection.commit()

#     return output
    
