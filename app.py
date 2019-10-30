import datetime, mohawk, numpy as np, os, sqlite3
from decouple import config
from flask import current_app, Flask, render_template, request
from flask.json import JSONEncoder
from etl.scheduler import Scheduler
from utils.utils import to_web_dict
from utils.sql import query_database
from authbroker_client import authbroker_blueprint, login_required

import os
print(os.getcwd())

from authentication import hawk_decorator_factory

import views
from db import get_db
import etl.views
import data_report
from etl.config import (
    countries_and_sectors_of_interest_table_name,
    countries_of_interest_table_name,
    datahub_company_id_to_companies_house_company_number_table_name,
    datahub_sector_table_name,
    export_countries_table_name,
    sectors_of_interest_table_name,
)

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
app.config['ABC_BASE_URL'] = config('ABC_BASE_URL', 'https://sso.trade.gov.uk')
app.config['ABC_CLIENT_ID'] = config('ABC_CLIENT_ID', 'get this from your network admin')
app.config['ABC_CLIENT_SECRET'] = config('ABC_CLIENT_SECRET', 'get this from your network admin')
app.secret_key = config('APP_SECRET_KEY', 'the random string')
app.register_blueprint(authbroker_blueprint)

app.config['HAWK_ENABLED'] = config(
    'HAWK_ENABLED', app.config['ENV'] in ('production', 'test'),
    cast=bool
)
app.json_encoder = CustomJSONEncoder
cf_port = os.getenv("PORT")

if app.config['ENV'] == 'production':
    app.config['DATABASE'] = os.environ['DATABASE_URL']
elif app.config['ENV'] in ['dev', 'development']:
    app.config['DATABASE'] = 'postgresql://countries_of_interest_service@localhost'\
        '/countries_of_interest_service'
elif app.config['ENV'] == 'test':
    app.config['DATABASE'] = 'postgresql://test_countries_of_interest_service@localhost'\
        '/test_countries_of_interest_service'
else:
    raise Exception('unrecognised environment')

# decorator for hawk authentication
# when hawk is disabled the authentication is trivial, effectively all requests are authenticated
hawk_authentication = hawk_decorator_factory(app.config['HAWK_ENABLED'])
    
@app.route('/api/v1/get-companies-affected-by-trade-barrier/<country>/<sector>')
@hawk_authentication
def get_companies_affected_by_trade_barrier(country, sector):
    sql_query = '''
select
  companies_house_company_number
  
from {table}

where country_of_interest = '{country}'
  and sector_of_interest = '{sector}'

'''.format(
    country=country,
    sector=sector,
    table=countries_and_sectors_of_interest_table_name
)
    with get_db() as connection:
        df = query_database(connection, sql_query)
    connection.close()
    return to_web_dict(df)

@app.route('/api/v1/get-companies-house-company-numbers')
@hawk_authentication
def get_companies_house_company_numbers():
    sql_query = '''
select distinct
  companies_house_company_number

from {table}

order by 1
'''.format(table=datahub_company_id_to_companies_house_company_number_table_name)
    with get_db() as connection:
        df = query_database(connection, sql_query)
    connection.close()
    return to_web_dict(df)

@app.route('/api/v1/get-company-countries-and-sectors-of-interest')
@hawk_authentication
def get_company_countries_and_sectors_of_interest():
    sql_query = '''
select
  companies_house_company_number,
  country_of_interest,
  sector_of_interest,
  source,
  source_id,
  timestamp

from {table}

'''.format(table=countries_and_sectors_of_interest_table_name)
    with get_db() as connection:
        df = query_database(connection, sql_query)
    connection.close()
    return to_web_dict(df)

@app.route('/api/v1/get-company-countries-of-interest')
@hawk_authentication
def get_company_countries_of_interest():
    sql_query = '''
select
  companies_house_company_number,
  country_of_interest,
  source,
  source_id,
  timestamp

from {table}

'''.format(table=countries_of_interest_table_name)
    with get_db() as connection:
        df = query_database(connection, sql_query)
    connection.close()
    return to_web_dict(df)

@app.route('/api/v1/get-company-export-countries')
@hawk_authentication
def get_company_export_countries():
    sql_query = '''
select
  companies_house_company_number,
  export_country,
  source,
  source_id,
  timestamp

from {table}

'''.format(table=export_countries_table_name)
    with get_db() as connection:
        df = query_database(connection, sql_query)
    connection.close()
    return to_web_dict(df)

@app.route('/api/v1/get-company-sectors-of-interest')
@hawk_authentication
def get_company_sectors_of_interest():
    sql_query = '''
select
  companies_house_company_number,
  sector_of_interest,
  source,
  source_id,
  timestamp

from {table}

order by 1, 3, 2
'''.format(table=sectors_of_interest_table_name)
    with get_db() as connection:
        df = query_database(connection, sql_query)
    connection.close()
    return to_web_dict(df)

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

from {table}
'''.format(table=datahub_company_id_to_companies_house_company_number_table_name)
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

from {table}
'''.format(table=datahub_company_id_to_companies_house_company_number_table_name)
    with get_db() as connection:
        df = query_database(connection, sql_query)
    connection.close()
    return to_web_dict(df)

@app.route('/')
@login_required
def get_index():
    return render_template('index.html')

@app.route('/api/v1/get-sectors')
@hawk_authentication
def get_sectors():
    sql_query = '''
select distinct
  sector

from {table}

order by 1

'''.format(table=datahub_sector_table_name)
    with get_db() as connection:
        df = query_database(connection, sql_query)
    connection.close()
    return to_web_dict(df)

@app.route('/api/v1/populate-database')
@hawk_authentication
def populate_database():
    return etl.views.populate_database()

scheduled_task = Scheduler()
scheduled_task.start()

if __name__ == '__main__':
    if cf_port is None:
        app.run(host='0.0.0.0', port=5000, debug=True)
    else:
        app.run(host='0.0.0.0', port=int(cf_port), debug=True)
