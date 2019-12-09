import datetime

from authbroker_client import login_required

from flask import jsonify, render_template, request

from flask.blueprints import Blueprint

from flask import current_app as flask_app

import pandas as pd


import data_report

from db import get_db

from utils import utils
from utils.sql import execute_query, query_database, table_exists
from utils.utils import to_web_dict

from app.api.tasks import populate_database_task

api = Blueprint(
    name="api",
    import_name=__name__
)


# views
@api.route('/api/v1/get-companies-house-company-numbers')
def get_companies_house_company_numbers():
    sql_query = '''
select distinct
  companies_house_company_number

from coi_datahub_company_id_to_companies_house_company_number

order by 1
'''
    with get_db() as connection:
        df = query_database(connection, sql_query)
    connection.close()
    return to_web_dict(df)


@api.route('/api/v1/get-company-countries-and-sectors-of-interest')
@utils.response_orientation_decorator
def get_company_countries_and_sectors_of_interest(orientation):
    pagination_size = flask_app.config['app']['pagination_size']
    next_source = request.args.get('next-source')
    next_source_id = request.args.get('next-source-id')
    company_ids = request.args.getlist('company-id')
    countries = request.args.getlist('country')
    sectors = request.args.getlist('sector')
    sources = request.args.getlist('source')

    where = ''
    values = countries + sectors + sources + company_ids
    if len(countries) == 1:
        where = 'where country_of_interest=%s'
    elif len(countries) > 1:
        where = (
            'where country_of_interest in ('
            + ','.join('%s' for i in range(len(countries)))
            + ')'
        )
    if len(sectors) == 1:
        where = where + ' and' if where != '' else 'where'
        where = where + ' sector_of_interest=%s'
    elif len(sectors) > 1:
        where = where + ' and' if where != '' else 'where'
        where = (
            where
            + ' sector_of_interest in ('
            + ','.join(['%s' for i in range(len(sectors))])
            + ')'
        )
    if len(company_ids) == 1:
        where = where + ' and' if where != '' else 'where'
        where = where + ' company_id=%s'
    elif len(company_ids) > 1:
        where = where + ' and' if where != '' else 'where'
        where = (
            where
            + ' company_id in ('
            + ','.join(['%s' for i in range(len(company_ids))])
            + ')'
        )
    if len(sources) == 1:
        where = where + ' and' if where != '' else 'where'
        where = where + ' source=%s'
    elif len(sources) > 1:
        where = where + ' and' if where != '' else 'where'
        where = (
            where + ' source in (' + ','.join(['%s' for i in range(len(sources))]) + ')'
        )
    if next_source is not None and next_source_id is not None:
        where = where + ' and' if where != '' else 'where'
        where = where + ' (source, source_id) >= (%s, %s)'
        values = values + [next_source, next_source_id]

    sql_query = '''
select
  company_id,
  country_of_interest,
  sector_of_interest,
  source,
  source_id,
  timestamp

from coi_countries_and_sectors_of_interest

{where}

order by (source, source_id)

limit {pagination_size} + 1

'''.format(
        where=where, pagination_size=pagination_size
    )
    with get_db() as connection:
        df = query_database(connection, sql_query, values)
    connection.close()
    if len(df) == pagination_size + 1:
        next_ = '{}{}?'.format(request.host_url[:-1], request.path)
        next_ += '&'.join(['country={}'.format(country) for country in countries])
        next_ += '&'.join(['source={}'.format(source) for source in sources])
        next_ += '&' if next_[-1] != '?' else ''
        next_ += 'next-source={}&next-source-id={}'.format(
            df['source'].values[-1], df['source_id'].values[-1],
        )
        df = df[:-1]
    else:
        next_ = None
    web_dict = to_web_dict(df, orientation)
    web_dict['next'] = next_
    return web_dict


@api.route('/api/v1/get-company-countries-of-interest')
@utils.response_orientation_decorator
def get_company_countries_of_interest(orientation):
    pagination_size = flask_app.config['app']['pagination_size']
    next_source = request.args.get('next-source')
    next_source_id = request.args.get('next-source-id')
    company_ids = request.args.getlist('company-id')
    countries = request.args.getlist('country')
    sources = request.args.getlist('source')

    values = countries + sources + company_ids
    where = ''
    if len(countries) == 1:
        where = 'where country_of_interest=%s'
    elif len(countries) > 1:
        where = (
            'where country_of_interest in ('
            + ','.join('%s' for i in range(len(countries)))
            + ')'
        )
    if len(company_ids) == 1:
        where = where + ' and' if where != '' else 'where'
        where = where + ' company_id=%s'
    elif len(company_ids) > 1:
        where = where + ' and' if where != '' else 'where'
        where = (
            where
            + ' company_id in ('
            + ','.join(['%s' for i in range(len(company_ids))])
            + ')'
        )
    if len(sources) == 1:
        where = where + ' and' if where != '' else 'where'
        where = where + ' source=%s'
    elif len(sources) > 1:
        where = where + ' and' if where != '' else 'where'
        where = (
            where + ' source in (' + ','.join(['%s' for i in range(len(sources))]) + ')'
        )
    if next_source is not None and next_source_id is not None:
        where = where + ' and' if where != '' else 'where'
        where = where + ' (source, source_id) >= (%s, %s)'
        values = values + [next_source, next_source_id]

    sql_query = '''
select
  company_id,
  country_of_interest,
  source,
  source_id,
  timestamp

from coi_countries_of_interest

{where}

order by (source, source_id)

limit {pagination_size} + 1

'''.format(
        pagination_size=pagination_size, where=where
    )
    with get_db() as connection:
        df = query_database(connection, sql_query, values)
    connection.close()
    if len(df) == pagination_size + 1:
        next_ = '{}{}'.format(request.host_url[:-1], request.path)
        next_ += '?'
        next_ += '&'.join(['country={}'.format(country) for country in countries])
        next_ += '&'.join(['source={}'.format(source) for source in sources])
        next_ += '&' if next_[-1] != '?' else ''
        next_ += 'next-source={}&next-source-id={}'.format(
            df['source'].values[-1], df['source_id'].values[-1],
        )
        df = df[:-1]
    else:
        next_ = None
    web_dict = to_web_dict(df, orientation)
    web_dict['next'] = next_
    return web_dict


@api.route('/api/v1/get-company-export-countries')
@utils.response_orientation_decorator
def get_company_export_countries(orientation):
    pagination_size = flask_app.config['app']['pagination_size']
    next_source = request.args.get('next-source')
    next_source_id = request.args.get('next-source-id')
    company_ids = request.args.getlist('company-id')
    countries = request.args.getlist('country')
    sources = request.args.getlist('source')

    values = countries + sources + company_ids
    where = ''
    if len(countries) == 1:
        where = 'where export_country=%s'
    elif len(countries) > 1:
        where = (
            'where export_country in ('
            + ','.join('%s' for i in range(len(countries)))
            + ')'
        )
    if len(company_ids) == 1:
        where = where + ' and' if where != '' else 'where'
        where = where + ' company_id=%s'
    elif len(company_ids) > 1:
        where = where + ' and' if where != '' else 'where'
        where = (
            where
            + ' company_id in ('
            + ','.join(['%s' for i in range(len(company_ids))])
            + ')'
        )
    if len(sources) == 1:
        where = where + ' and' if where != '' else 'where'
        where = where + ' source=%s'
    elif len(sources) > 1:
        where = where + ' and' if where != '' else 'where'
        where = (
            where + ' source in (' + ','.join(['%s' for i in range(len(sources))]) + ')'
        )
    if next_source is not None and next_source_id is not None:
        where = where + ' and' if where != '' else 'where'
        where = where + ' (source, source_id) >= (%s, %s)'
        values = values + [next_source, next_source_id]

    sql_query = '''
select
  company_id,
  export_country,
  source,
  source_id,
  timestamp

from coi_export_countries

{where}

order by (source, source_id)

limit {pagination_size} + 1

'''.format(
        where=where, pagination_size=pagination_size
    )
    with get_db() as connection:
        df = query_database(connection, sql_query, values)
    connection.close()
    if len(df) == pagination_size + 1:
        next_ = '{}{}'.format(request.host_url[:-1], request.path)
        next_ += '?'
        next_ += '&'.join(['country={}'.format(country) for country in countries])
        next_ += '&'.join(['source={}'.format(source) for source in sources])
        next_ += '&' if next_[-1] != '?' else ''
        next_ += 'next-source={}&next-source-id={}'.format(
            df['source'].values[-1], df['source_id'].values[-1],
        )
        df = df[:-1]
    else:
        next_ = None
    web_dict = to_web_dict(df, orientation)
    web_dict['next'] = next_
    return web_dict


@api.route('/api/v1/get-company-sectors-of-interest')
@utils.response_orientation_decorator
def get_company_sectors_of_interest(orientation):
    pagination_size = flask_app.config['app']['pagination_size']
    next_source = request.args.get('next-source')
    next_source_id = request.args.get('next-source-id')
    company_ids = request.args.getlist('company-id')
    sectors = request.args.getlist('sector')
    sources = request.args.getlist('source')

    values = sectors + sources + company_ids
    where = ''
    if len(sectors) == 1:
        where = 'where sector_of_interest=%s'
    elif len(sectors) > 1:
        where = (
            'where sector_of_interest in ('
            + ','.join('%s' for i in range(len(sectors)))
            + ')'
        )
    if len(company_ids) == 1:
        where = where + ' and' if where != '' else 'where'
        where = where + ' company_id=%s'
    elif len(company_ids) > 1:
        where = where + ' and' if where != '' else 'where'
        where = (
            where
            + ' company_id in ('
            + ','.join(['%s' for i in range(len(company_ids))])
            + ')'
        )
    if len(sources) == 1:
        where = where + ' and' if where != '' else 'where'
        where = where + ' source=%s'
    elif len(sources) > 1:
        where = where + ' and' if where != '' else 'where'
        where = (
            where + ' source in (' + ','.join(['%s' for i in range(len(sectors))]) + ')'
        )
    if next_source is not None and next_source_id is not None:
        where = where + ' and' if where != '' else 'where'
        where = where + ' (source, source_id) >= (%s, %s)'
        values = values + [next_source, next_source_id]

    sql_query = '''
select
  company_id,
  sector_of_interest,
  source,
  source_id,
  timestamp

from coi_sectors_of_interest

{where}

order by (source, source_id)

limit {pagination_size} + 1

'''.format(
        where=where, pagination_size=pagination_size
    )
    with get_db() as connection:
        df = query_database(connection, sql_query, values)
    connection.close()

    if len(df) == pagination_size + 1:
        next_ = '{}{}'.format(request.host_url[:-1], request.path)
        next_ += '?'
        next_ += '&'.join(['sector={}'.format(sector) for sector in sectors])
        next_ += '&'.join(['source={}'.format(source) for source in sources])
        next_ += '&' if next_[-1] != '?' else ''
        next_ += 'next-source={}&next-source-id={}'.format(
            df['source'].values[-1], df['source_id'].values[-1],
        )
        df = df[:-1]
    else:
        next_ = None
    web_dict = to_web_dict(df, orientation)
    web_dict['next'] = next_
    return web_dict


@api.route('/data-report')
@login_required
def get_data_report():
    return render_template('data_report.html')


@api.route('/api/v1/get-data-report-data')
def get_data_report_data():
    return data_report.get_data_report_data()


@api.route('/api/v1/get-datahub-company-ids')
def get_datahub_company_ids():
    sql_query = '''
select distinct
  datahub_company_id

from coi_datahub_company_id_to_companies_house_company_number
'''
    with get_db() as connection:
        df = query_database(connection, sql_query)
    connection.close()
    return to_web_dict(df)


@api.route('/api/v1/get-datahub-company-ids-to-companies-house-company-numbers')
def get_datahub_company_ids_to_companies_house_company_numbers():
    sql_query = '''
select
  datahub_company_id,
  companies_house_company_number

from coi_datahub_company_id_to_companies_house_company_number
'''
    with get_db() as connection:
        df = query_database(connection, sql_query)
    connection.close()
    return to_web_dict(df)


@api.route('/')
@login_required
def get_index():
    last_updated = None
    with get_db() as connection:
        if table_exists(connection, 'etl_runs'):
            sql = 'select max(timestamp) from etl_runs'
            df = query_database(connection, sql)
            last_updated = pd.to_datetime(df.values[0][0])
    if last_updated is None:
        last_updated = 'Database not yet initialised'
    else:
        last_updated = last_updated.strftime('%Y-%m-%d %H:%M:%S')
    return render_template('index.html', last_updated=last_updated)


@api.route('/api/v1/get-sectors')
def get_sectors():
    sql_query = '''
select distinct
  sector

from datahub_sector

order by 1

'''
    with get_db() as connection:
        df = query_database(connection, sql_query)
    connection.close()
    return to_web_dict(df)


@api.route('/api/v1/populate-database')
def populate_database():
    drop_table = 'drop-table' in request.args
    force_update = 'force-update' in request.args
    with get_db() as connection:
        sql = (
            'create table if not exists etl_status '
            '(status varchar(100), timestamp timestamp)'
        )
        execute_query(connection, sql)
        sql = 'select * from etl_status'
        df = query_database(connection, sql)
    if force_update is True or len(df) == 0 or df['status'].values[0] == 'SUCCESS':
        populate_database_task.delay(drop_table)
        sql = 'delete from etl_status'
        execute_query(connection, sql)
        sql = '''insert into etl_status values (%s, %s)'''
        execute_query(connection, sql, values=['RUNNING', datetime.datetime.now()])
        return {'status': 200, 'message': 'started populate_database task'}
    else:
        return {
            'status': 200,
            'message': ('populate_database task already running since: {}').format(
                df['timestamp'].values[0]
            ),
        }


@api.route('/healthcheck/', methods=["GET"])
def healthcheck():
    return jsonify({"status": "OK"})
