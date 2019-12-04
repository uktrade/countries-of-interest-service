import datetime, json, logging, os, sqlite3
import mohawk, numpy as np, pandas as pd, redis
from celery import Celery
from decouple import config
from flask import current_app, Flask, render_template, request
from flask.json import JSONEncoder
from flask import jsonify
from authbroker_client import authbroker_blueprint, login_required

import data_report
import etl.tasks.core
from authentication import hawk_decorator_factory
from db import get_db
from etl.scheduler import Scheduler
from utils import utils
from utils.utils import to_web_dict
from utils.sql import execute_query, query_database, table_exists


class CustomJSONEncoder(JSONEncoder):
    
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        elif isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(MyEncoder, self).default(obj)

app = Flask(__name__)

# sso
app.register_blueprint(authbroker_blueprint)
app.config['ABC_BASE_URL'] = config('ABC_BASE_URL', 'https://sso.trade.gov.uk')
app.config['ABC_CLIENT_ID'] = config('ABC_CLIENT_ID', 'get this from your network admin')
app.config['ABC_CLIENT_SECRET'] = config('ABC_CLIENT_SECRET', 'get this from your network admin')

# other
app.secret_key = config('APP_SECRET_KEY', 'the random string')
app.json_encoder = CustomJSONEncoder
assert app.config['ENV'] in ('production', 'dev', 'development', 'test'), \
    'invalid environment: {}'.format(app.config['ENV'])
app.config['DATAWORKSPACE_HOST'] = config('DATAWORKSPACE_HOST', 'localapps.com:8000')
app.config['PAGINATION_SIZE'] = config('PAGINATION_SIZE', 50, cast=int)
app.config['RUN_SCHEDULER'] = config('RUN_SCHEDULER', False, cast=bool)

# app database
if app.config['ENV'] == 'production':
    app.config['DATABASE'] = os.environ['DATABASE_URL']
elif app.config['ENV'] in ['dev', 'development']:
    app.config['DATABASE'] = config(
        'DATABASE_URL',
        'postgresql://countries_of_interest_service@localhost/countries_of_interest_service'
    )
elif app.config['ENV'] == 'test':
    app.config['DATABASE'] = config(
        'DATABASE_URL',
        'postgresql://test_countries_of_interest_service@localhost' \
        '/test_countries_of_interest_service',
    )
else:
    raise Exception('unrecognised environment')

# celery & cloud foundry config
vcap_services = os.environ.get('VCAP_SERVICES')
if vcap_services:
    vacap_services = json.loads(vcap_services)
    redis_config = json.loads(vcap_services)['redis'][0]
    redis_uri = redis_config['credentials']['uri']
app.config['CELERY_BROKER'] = config(
    'CELERY_BROKER',
    redis_uri if vcap_services else 'redis://localhost'
)

# celery, tasks config
celery = Celery('app', broker=app.config['CELERY_BROKER'])
@celery.task
def populate_database_task(drop_table=True):
    with app.app_context():
        return etl.tasks.core.populate_database(drop_table)

# hawk authentication
app.config['HAWK_ENABLED'] = config(
    'HAWK_ENABLED', app.config['ENV'] in ('production', 'test'),
    cast=bool
)
app.config['DATAFLOW_HAWK_CLIENT_ID'] = config('DATAFLOW_HAWK_CLIENT_ID', 'dataflow_client_id')
app.config['DATAFLOW_HAWK_CLIENT_KEY'] = config('DATAFLOW_HAWK_CLIENT_KEY', 'dataflow_client_key')
app.config['DATAWORKSPACE_HAWK_CLIENT_ID'] = config(
    'DATAWORKSPACE_HAWK_CLIENT_ID',
    'dataworkspace_client_id'
)
app.config['DATAWORKSPACE_HAWK_CLIENT_KEY'] = config(
    'DATAWORKSPACE_HAWK_CLIENT_KEY',
    'dataworkspace_client_key'
)
# decorator for hawk authentication
# when hawk is disabled the authentication is trivial, effectively all requests are authenticated
hawk_authentication = hawk_decorator_factory(app.config['HAWK_ENABLED'])
users = [
    (
        config('DATAFLOW_HAWK_CLIENT_ID', 'dataflow_client_id'),
        config('DATAFLOW_HAWK_CLIENT_KEY', 'dataflow_client_key'),
    ),
]
def create_users_table(users):
    with app.app_context():
        sql = 'drop table if exists users'
        with get_db() as connection:
            execute_query(connection, sql)
        sql = 'create table users (' \
            'client_id varchar(100) primary key,' \
            'client_key varchar(100)' \
            ')'
        with get_db() as connection:
            execute_query(connection, sql)
        sql = 'insert into users values (%s, %s) ' 
        with get_db() as connection:
            cursor = connection.cursor()
            cursor.executemany(sql, users)
if app.config['ENV'] != 'test':
    create_users_table(users)

# views
@app.route('/api/v1/get-companies-house-company-numbers')
@hawk_authentication
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

@app.route('/api/v1/get-company-countries-and-sectors-of-interest')
@hawk_authentication
@utils.response_orientation_decorator
def get_company_countries_and_sectors_of_interest(orientation):
    pagination_size = app.config['PAGINATION_SIZE']
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
        where = 'where country_of_interest in (' + ','.join(
            '%s' for i in range(len(countries))
        ) + ')'
    if len(sectors) == 1:
        where = where + ' and' if where != '' else 'where'
        where = where + ' sector_of_interest=%s'
    elif len(sectors) > 1:
        where = where + ' and' if where != '' else 'where'
        where = where + ' sector_of_interest in (' + ','.join(
            ['%s' for i in range(len(sectors))]
        ) + ')'
    if len(company_ids) == 1:
        where = where + ' and' if where != '' else 'where'
        where = where + ' company_id=%s'
    elif len(company_ids) > 1:
        where = where + ' and' if where != '' else 'where'
        where = where + ' company_id in (' + ','.join(
            ['%s' for i in range(len(company_ids))]
        ) + ')'
    if len(sources) == 1:
        where = where + ' and' if where != '' else 'where'
        where = where + ' source=%s'
    elif len(sources) > 1:
        where = where + ' and' if where != '' else 'where'
        where = where + ' source in (' + ','.join(
            ['%s' for i in range(len(sectors))]
        ) + ')'
    if (next_source is not None and next_source_id is not None):
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

'''.format(where=where, pagination_size=pagination_size)
    with get_db() as connection:
        df = query_database(connection, sql_query, values)
    connection.close()
    if len(df) == pagination_size + 1:
        next_ = '{}{}?'.format(request.host_url[:-1], request.path)
        next_ += '&'.join(['country={}'.format(country) for country in countries])
        next_ += '&'.join(['source={}'.format(source) for source in sources])
        next_ += '&' if next_[-1] != '?' else ''
        next_ += 'next-source={}&next-source-id={}'.format(
            df['source'].values[-1],
            df['source_id'].values[-1],
        )
        df = df[:-1]
    else:
        next_ = None
    web_dict = to_web_dict(df, orientation)
    web_dict['next'] = next_
    return web_dict

@app.route('/api/v1/get-company-countries-of-interest')
@hawk_authentication
@utils.response_orientation_decorator
def get_company_countries_of_interest(orientation):
    pagination_size = app.config['PAGINATION_SIZE']
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
        where = 'where country_of_interest in (' + ','.join(
            '%s' for i in range(len(countries))
        ) + ')'
    if len(company_ids) == 1:
        where = where + ' and' if where != '' else 'where'
        where = where + ' company_id=%s'
    elif len(company_ids) > 1:
        where = where + ' and' if where != '' else 'where'
        where = where + ' company_id in (' + ','.join(
            ['%s' for i in range(len(company_ids))]
        ) + ')'
    if len(sources) == 1:
        where = where + ' and' if where != '' else 'where'
        where = where + ' source=%s'
    elif len(sources) > 1:
        where = where + ' and' if where != '' else 'where'
        where = where + ' source in (' + ','.join(
            ['%s' for i in range(len(sectors))]
        ) + ')'
    if (next_source is not None and next_source_id is not None):
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

'''.format(pagination_size=pagination_size, where=where)
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
            df['source'].values[-1],
            df['source_id'].values[-1],
        )
        df = df[:-1]
    else:
        next_ = None
    web_dict = to_web_dict(df, orientation)
    web_dict['next'] = next_
    return web_dict


@app.route('/api/v1/get-company-export-countries')
@hawk_authentication
@utils.response_orientation_decorator
def get_company_export_countries(orientation):
    pagination_size = app.config['PAGINATION_SIZE']
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
        where = 'where export_country in (' + ','.join(
            '%s' for i in range(len(countries))
        ) + ')'
    if len(company_ids) == 1:
        where = where + ' and' if where != '' else 'where'
        where = where + ' company_id=%s'
    elif len(company_ids) > 1:
        where = where + ' and' if where != '' else 'where'
        where = where + ' company_id in (' + ','.join(
            ['%s' for i in range(len(company_ids))]
        ) + ')'
    if len(sources) == 1:
        where = where + ' and' if where != '' else 'where'
        where = where + ' source=%s'
    elif len(sources) > 1:
        where = where + ' and' if where != '' else 'where'
        where = where + ' source in (' + ','.join(
            ['%s' for i in range(len(sectors))]
        ) + ')'
    if (next_source is not None and next_source_id is not None):
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

'''.format(where=where, pagination_size=pagination_size)
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
            df['source'].values[-1],
            df['source_id'].values[-1],
        )
        df = df[:-1]
    else:
        next_ = None
    web_dict = to_web_dict(df, orientation)
    web_dict['next'] = next_
    return web_dict

@app.route('/api/v1/get-company-sectors-of-interest')
@hawk_authentication
@utils.response_orientation_decorator
def get_company_sectors_of_interest(orientation):
    pagination_size = app.config['PAGINATION_SIZE']
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
        where = 'where sector_of_interest in (' + ','.join(
            '%s' for i in range(len(countries))
        ) + ')'
    if len(company_ids) == 1:
        where = where + ' and' if where != '' else 'where'
        where = where + ' company_id=%s'
    elif len(company_ids) > 1:
        where = where + ' and' if where != '' else 'where'
        where = where + ' company_id in (' + ','.join(
            ['%s' for i in range(len(company_ids))]
        ) + ')'
    if len(sources) == 1:
        where = where + ' and' if where != '' else 'where'
        where = where + ' source=%s'
    elif len(sources) > 1:
        where = where + ' and' if where != '' else 'where'
        where = where + ' source in (' + ','.join(
            ['%s' for i in range(len(sectors))]
        ) + ')'
    if (next_source is not None and next_source_id is not None):
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

'''.format(where=where, pagination_size=pagination_size)
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
            df['source'].values[-1],
            df['source_id'].values[-1],
        )
        df = df[:-1]
    else:
        next_ = None
    web_dict = to_web_dict(df, orientation)
    web_dict['next'] = next_
    return web_dict

@app.route('/data-report')
@login_required
def get_data_report():
    return render_template('data_report.html')

@app.route('/api/v1/get-data-report-data')
@hawk_authentication
def get_data_report_data():
    return data_report.get_data_report_data()

@app.route('/api/v1/get-datahub-company-ids')
@hawk_authentication
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

@app.route('/api/v1/get-datahub-company-ids-to-companies-house-company-numbers')
@hawk_authentication
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

@app.route('/')
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

@app.route('/api/v1/get-sectors')
@hawk_authentication
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

@app.route('/api/v1/populate-database')
@hawk_authentication
def populate_database():
    drop_table = 'drop-table' in request.args
    force_update = 'force-update' in request.args
    with get_db() as connection:
        sql = 'create table if not exists etl_status (status varchar(100), timestamp timestamp)'
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
            'message': 'populate_database task already running since: {}'.format(
                df['timestamp'].values[0]
            )
        }


@app.route('/healthcheck/', methods=["GET"])
def healthcheck():
    return jsonify({
        "status": "OK"
    })


if app.config['RUN_SCHEDULER'] is True:
    print('starting scheduler')
    scheduled_task = Scheduler(populate_database_task)
    scheduled_task.start()

