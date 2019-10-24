import mohawk, os, psycopg2, requests
from db import get_db
from utils import sql as sql_utils


dataworkspace_host = os.environ['DATAWORKSPACE_HOST']
client_id = os.environ['DATAWORKSPACE_HAWK_CLIENT_ID']
client_key = os.environ['DATAWORKSPACE_HAWK_CLIENT_KEY']
algorithm = 'sha256'
method = 'GET'
content = ''
content_type = ''
credentials = {
    'id': client_id,
    'key': client_key,
    'algorithm': algorithm
}

def populate_table(url, table_name, schema, primary_key=None, stubbed_data={}):
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

    # TODO: connect to data-workspace
    # reponse = requests.get(url, headers=headers)
    # data = response.json()
    data = stubbed_data

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
        output = "{} record(s) inserted successfully into {} table".format(
            cursor.rowcount,
            table_name
        )
        print(output)

    except (Exception, psycopg2.Error) as error:
        output = "Failed inserting record into {} table {}".format(table_name, error)
        print(output)

        if table_exists:
            sql = ''' alter table {} rename to {} '''.format(table_name_backup, table_name)
            cursor.execute(sql)
            connection.commit()

    return output

def populate_countries_and_sectors_of_interest():
    primary_key = (
        'companies_house_company_number',
        'country_of_interest',
        'sector_of_interest',
        'source',
        'source_id'
    )
    schema = [
        'companies_house_company_number varchar(12)',
        'country_of_interest varchar(12)',
        'sector_of_interest varchar(50)',
        'source varchar(50)',
        'source_id varchar(100)',
        'timestamp timestamp',
    ]
    stubbed_data = {
        'headers': [
            'companiesHouseCompanyNumber',
            'countryOfInterest',
            'sectorOfInterest',
            'source',
            'sourceId',
            'timestamp'
        ],
        'values': [
            ['1', 'CN', 'Aerospace', 'OMIS', 'asdf', '2019-08-01'],
            ['2', 'US', 'Engineering', 'OMIS', 'asdfg', '2019-08-02'],
        ]
    }
    table_name = 'countries_and_sectors_of_interest'
    url = 'https://{dataworkspace_host}/api/v1/get-countries-and-sectors-of-interest'.format(
        dataworkspace_host=dataworkspace_host
    )
    return populate_table(url, table_name, schema, primary_key, stubbed_data)

def populate_countries_of_interest():
    primary_key = (
        'companies_house_company_number',
        'country_of_interest',
        'source',
        'source_id'
    )
    schema = [
        'companies_house_company_number varchar(12)',
        'country_of_interest varchar(12)',
        'source varchar(50)',
        'source_id varchar(100)',
        'timestamp timestamp',
    ]
    stubbed_data = {
        'headers': [
            'companiesHouseCompanyNumber',
            'countryOfInterest',
            'source',
            'sourceId',
            'timestamp'
        ],
        'values': [
            ['1', 'CN', 'OMIS', 'asdf', '2019-08-01'],
            ['2', 'US', 'OMIS', 'asdfg', '2019-08-02'],
        ]
    }
    table_name = 'countries_of_interest'
    url = 'https://{dataworkspace_host}/api/v1/get-countries-of-interest'.format(
        dataworkspace_host=dataworkspace_host
    )
    return populate_table(url, table_name, schema, primary_key, stubbed_data)

def populate_datahub_company_id_to_companies_house_company_number():
    endpoint = 'get-datahub-company-id-to-companies-house-company-id'
    url = 'https://{dataworkspace_host}/api/v1/{endpoint}'.format(
        dataworkspace_host=dataworkspace_host,
        endpoint=endpoint
    )
    primary_key = 'datahub_company_id'
    schema = [
        'datahub_company_id uuid',
        'companies_house_company_number varchar(12)'
    ]
    stubbed_data = {
        'headers': [
            'datahubCompanyId',
            'companiesHouseCompanyNumber',
        ],
        'values': [
            ['48edbf85-de62-4f6c-b1c5-9f056be1b391', 'company1'],
            ['c3b277d0-ceb4-4327-8cc1-f55fbcbff13b', 'company2'],
        ]
    }
    table_name = 'datahub_company_id_to_companies_house_company_number'
    return populate_table(url, table_name, schema, primary_key, stubbed_data)

def populate_export_countries():
    endpoint = 'get-export-countries'
    url = 'https://{dataworkspace_host}/api/v1/{endpoint}'.format(
        dataworkspace_host=dataworkspace_host,
        endpoint=endpoint
    )
    primary_key = [
        'companies_house_company_number',
        'export_country',
        'source',
        'source_id'
    ]
    schema = [
        'companies_house_company_number varchar(12)',
        'export_country varchar(12)',
        'source varchar(50)',
        'source_id varchar(100)',
        'timestamp timestamp',
    ]
    stubbed_data = {
        'headers': [
            'companiesHouseCompanyNumber',
            'exportCountry',
            'source',
            'sourceId',
            'timestamp'
        ],
        'values': [
            ['company1', 'US', 'datahub_company', '0', None],
            ['company2', 'FR', 'datahub_company', '1', None],
        ]
    }
    table_name = 'export_countries'
    return populate_table(url, table_name, schema, primary_key, stubbed_data)

def populate_sectors():
    endpoint = 'get-sectors'
    url = 'https://{dataworkspace_host}/api/v1/{endpoint}'.format(
        dataworkspace_host=dataworkspace_host,
        endpoint=endpoint
    )
    primary_key = 'sector'
    schema = ['sector varchar(200)']
    stubbed_data = {
        'headers': ['sector',],
        'values': ['Aerospace', 'Engineering']
    }
    table_name = 'sectors'
    return populate_table(url, table_name, schema, primary_key, stubbed_data)

def populate_sectors_of_interest():
    primary_key = (
        'companies_house_company_number',
        'sector_of_interest',
        'source',
        'source_id'
    )
    schema = [
        'companies_house_company_number varchar(12)',
        'sector_of_interest varchar(200)',
        'source varchar(50)',
        'source_id varchar(100)',
        'timestamp timestamp',
    ]
    stubbed_data = {
        'headers': [
            'companiesHouseCompanyNumber',
            'sectorOfInterest',
            'source',
            'sourceId',
            'timestamp'
        ],
        'values': [
            ['1', 'Aerospace', 'OMIS', 'asdf', '2019-08-01'],
            ['2', 'Engineering', 'OMIS', 'asdfg', '2019-08-02'],
        ]
    }
    table_name = 'sectors_of_interest'
    url = 'https://{dataworkspace_host}/api/v1/get-sectors-of-interest'.format(
        dataworkspace_host=dataworkspace_host
    )
    return populate_table(url, table_name, schema, primary_key, stubbed_data)
    
