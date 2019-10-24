import datetime, mohawk, numpy as np, os, sqlite3
from decouple import config
from flask import current_app, Flask, render_template, request
from flask.json import JSONEncoder
from utils.utils import to_web_dict
from utils.sql import query_database
from authbroker_client import authbroker_blueprint, login_required
from datapipeline.scheduler import Scheduler

import views
from db import get_db, query_db
import datapipeline.views

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

app.config['HAWK_ENABLED'] = config('HAWK_ENABLED', app.config['ENV'] == 'production', cast=bool)
app.json_encoder = CustomJSONEncoder
cf_port = os.getenv("PORT")

if app.config['ENV'] == 'production':
    app.config['DATABASE'] = os.environ['DATABASE_URL']
elif app.config['ENV'] in ['dev', 'development']:
    app.config['DATABASE'] = 'postgresql://countries_of_interest_service@localhost'\
        '/countries_of_interest_service'
elif app.config['ENV'] == 'test':
    app.config['DATABASE'] = 'postgresql://countries_of_interest_service@localhost'\
        '/test_countries_of_interest_service'
else:
    raise Exception('unrecognised environment')

def lookup_hawk_credentials(user_id):
    sql = ''' select * from users where user_id='{user_id}' '''.format(user_id=user_id)
    connection = get_db()
    try:
        df = query_database(connection, sql)
        user_key = df['user_key'].values[0]
    except Exception as e:
        raise LookupError()
    
    return {
        'id': user_id,
        'key': user_key,
        'algorithm': 'sha256'
    }

def hawk_required(view, *args, **kwargs):
    if app.config['HAWK_ENABLED'] == False:
        return view

    def wrapper(*args, **kwargs):
        url = request.url
        method = request.method
        content = request.data
        content_type = request.content_type
        request_header = request.headers['Authorization']
    
        receiver = mohawk.Receiver(
            lookup_hawk_credentials,
            request_header=request.headers['Authorization'],
            url=url,
            method=method,
            content=content,
            content_type=content_type
        )
        return view(*args, **kwargs)
    wrapper.__name__ = view.__name__
    return wrapper

@app.route('/')
@hawk_required
@login_required
def get_index():
    return render_template('index.html')

@app.route('/api/get-data-report-data')
@hawk_required
def get_data_report_data():
    return datapipeline.views.get_data_report_data()
    
@app.route('/get-companies-affected-by-trade-barrier/<country>/<sector>')
@hawk_required
def get_companies_affected_by_trade_barrier(country, sector):
    sql_query = '''
select
  companies_house_company_number
  
from countries_and_sectors_of_interest

where country_of_interest = '{country}'
  and sector_of_interest = '{sector}'

'''.format(country=country, sector=sector)
    db = get_db()
    rows = query_db(db, sql_query)
    return {
        'headers': ['companiesHouseCompanyNumber'],
        'data': [r[0] for r in rows]
    }

@app.route('/get-companies-house-company-numbers')
@hawk_required
def get_companies_house_company_numbers():
    sql_query = '''
select distinct
  companies_house_company_number

from datahub_company_id_to_companies_house_company_number

order by 1
'''
    db = get_db()
    rows = query_db(db, sql_query)
    return {
        'headers': ['companiesHouseCompanyNumber'],
        'data': [r[0] for r in rows]
    }

@app.route('/get-company-countries-and-sectors-of-interest')
@hawk_required
def get_company_countries_and_sectors_of_interest():
    sql_query = '''
select
  companies_house_company_number,
  country_of_interest,
  sector_of_interest,
  source,
  source_id,
  timestamp

from countries_and_sectors_of_interest

'''
    db = get_db()
    rows = query_db(db, sql_query)
    return {
        'headers': [
            'companiesHouseCompanyNumber',
            'countryOfInterest',
            'sectorOfIntereset',
            'source',
            'sourceID',
            'timestamp'
        ],
        'data': [tuple(r) for r in rows]
    }

@app.route('/get-company-countries-of-interest')
def get_company_countries_of_interest():
    sql_query = '''
select
  companies_house_company_number,
  country_of_interest,
  source,
  source_id,
  timestamp

from countries_of_interest

'''
    db = get_db()
    rows = query_db(db, sql_query)
    return {
        'headers': [
            'companiesHouseCompanyNumber',
            'countryOfInterest',
            'source',
            'sourceId',
            'timestamp'
        ],
        'data': [tuple(r) for r in rows]
    }

@app.route('/get-company-export-countries')
@hawk_required
def get_company_export_countries():
    sql_query = '''
select
  companies_house_company_number,
  export_country,
  source,
  source_id,
  timestamp

from export_countries

'''
    db = get_db()
    rows = query_db(db, sql_query)
    return {
        'headers': [
            'companiesHouseCompanyNumber',
            'exportCountry',
            'source',
            'sourceID',
            'timestamp'
        ],
        'data': [tuple(r) for r in rows]
    }

@app.route('/get-company-sectors-of-interest')
@hawk_required
def get_company_sectors_of_interest():
    sql_query = '''
select
  companies_house_company_number,
  sector_of_interest,
  source,
  source_id,
  timestamp

from sectors_of_interest

order by 1, 3, 2
'''
    connection = get_db()
    df = query_database(connection, sql_query)
    web_dict = to_web_dict(df)
    web_dict['data'] = web_dict['data']
    return web_dict
    

@app.route('/get-datahub-company-ids')
@hawk_required
def get_datahub_company_ids():
    sql_query = '''
select distinct
  datahub_company_id

from datahub_company_id_to_companies_house_company_number
'''
    db = get_db()
    rows = query_db(db, sql_query)
    return {
        'headers': ['datahubCompanyID'],
        'data': [r[0] for r in rows]
    }

@app.route('/get-datahub-company-ids-to-companies-house-company-numbers')
@hawk_required
def get_datahub_company_ids_to_companies_house_company_numbers():
    sql_query = '''
select 
  datahub_company_id,
  companies_house_company_number

from datahub_company_id_to_companies_house_company_number
'''
    db = get_db()
    rows = query_db(db, sql_query)
    return {
        'headers': ['datahubCompanyID', 'companiesHouseCompanyNumber'],
        'data': rows
    }

@app.route('/get-sectors')
@hawk_required
def get_sectors():
    sql_query = '''
select distinct
  sector

from sectors

order by 1

'''
    db = get_db()
    rows = query_db(db, sql_query)
    return {
        'headers': ['sector'],
        'data': [r[0] for r in rows]
    }

@app.route('/api/populate-database')
@hawk_required
def populate_database():
    return datapipeline.views.populate_database()

@app.route('/data-report')
@hawk_required
def data_report():
    return render_template('data_report.html')

scheduled_task = Scheduler()
scheduled_task.start()

if __name__ == '__main__':
    if cf_port is None:
        app.run(host='0.0.0.0', port=5000, debug=True)
    else:
        app.run(host='0.0.0.0', port=int(cf_port), debug=True)
