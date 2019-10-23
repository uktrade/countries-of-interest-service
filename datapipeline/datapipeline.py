import mohawk, os, psycopg2, requests
from db import get_db
from utils import sql as sql_utils
from datapipeline.config import client_id, client_key, dataworkspace_host

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

def populate_countries_and_sectors_of_interest():
    url = 'https://{dataworkspace_host}/api/v1/get-countries-and-sectors-of-interest'.format(
        dataworkspace_host=dataworkspace_host
    )
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
    data = {
        'headers': [
            'companiesHouseCompanyNumber',
            'countryOfInterestId',
            'sectorSegment',
            'source',
            'sourceId',
            'timestamp'
        ],
        'values': [
            ['1', 'CN', 'Aerospace', 'OMIS', 'asdf', '2019-08-01'],
            ['2', 'US', 'Engineering', 'OMIS', 'asdfg', '2019-08-02'],
        ]
    }

    connection = get_db()
    cursor = connection.cursor()
    table_name = 'countries_and_sectors_of_interest'
    table_exists = sql_utils.table_exists(connection, table_name)

    try:
        if table_exists:
            table_name_backup = '{}_backup'.format(table_name)
            sql_utils.drop_table(connection, table_name_backup)
            sql_utils.rename_table(connection, table_name, table_name_backup)
        # TODO can this schema rule set be moved to a common location
        sql = ''' create table countries_and_sectors_of_interest (
        companies_house_company_number varchar(12), 
        country_of_interest_id varchar(12), 
        sector_segment varchar(50), 
        source varchar(50), 
        source_id varchar(100),
        timestamp timestamp,
        primary key (companies_house_company_number, country_of_interest_id, source, source_id)
        ) '''
        cursor.execute(sql)
        sql = ''' insert into countries_and_sectors_of_interest values (%s, %s, %s, %s, %s, %s) '''
        result = cursor.executemany(sql, data['values'])
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


def populate_countries_of_interest():
    url = 'https://{dataworkspace_host}/api/v1/get-countries-of-interest'.format(
        dataworkspace_host=dataworkspace_host
    )

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
    data = {
        'headers': [
            'companiesHouseCompanyNumber',
            'countryOfInterestId',
            'source',
            'sourceId',
            'timestamp'
        ],
        'values': [
            ['1', 'CN', 'OMIS', 'asdf', '2019-08-01'],
            ['2', 'US', 'OMIS', 'asdfg', '2019-08-02'],
        ]
    }

    connection = get_db()
    cursor = connection.cursor()
    table_name = 'countries_of_interest'
    table_exists = sql_utils.table_exists(connection, table_name)

    try:
        if table_exists:
            table_name_backup = '{}_backup'.format(table_name)
            sql_utils.drop_table(connection, table_name_backup)
            sql_utils.rename_table(connection, table_name, table_name_backup)
        # TODO can this schema rule set be moved to a common location
        sql = ''' create table countries_of_interest (
        companies_house_company_number varchar(12), 
        country_of_interest_id varchar(12), 
        source varchar(50), 
        source_id varchar(100),
        timestamp timestamp,
        primary key (companies_house_company_number, country_of_interest_id, source, source_id)
        ) '''
        cursor.execute(sql)
        sql = '''insert into countries_of_interest values (%s, %s, %s, %s, %s) ''' \
            '''on conflict do nothing'''
        result = cursor.executemany(sql, data['values'])
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
