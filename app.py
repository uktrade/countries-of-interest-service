import datetime, os, sqlite3
from db import get_db, query_db
from flask import Flask, render_template
from flask.json import JSONEncoder

cf_port = os.getenv("PORT")

class CustomJSONEncoder(JSONEncoder):

    def default(self, obj):
        try:
            if isinstance(obj, datetime.datetime):
                return obj.isoformat()
            iterable = iter(obj)
        except TypeError:
            pass
        else:
            return list(iterable)
        return JSONEncoder.default(self, obj)

app = Flask(__name__)
app.json_encoder = CustomJSONEncoder

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

@app.route('/')
def get_index():
    return render_template('index.html')
    
@app.route('/get-companies-affected-by-trade-barrier/<country>/<sector>')
def get_companies_affected_by_trade_barrier(country, sector):
    sql_query = '''
select
  companies_house_company_number
  
from countries_and_sectors_of_interest_by_companies_house_company_number

where country_of_interest_id = '{country}'
  and sector_segment = '{sector}'

'''.format(country=country, sector=sector)
    db = get_db()
    rows = query_db(db, sql_query)
    return {
        'headers': ['companiesHouseCompanyNumber'],
        'data': [r[0] for r in rows]
    }

@app.route('/get-companies-house-company-numbers')
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
def get_company_countries_and_sectors_of_interest():
    sql_query = '''
select
  companies_house_company_number,
  country_of_interest_id,
  sector_segment,
  source,
  source_id,
  timestamp

from countries_and_sectors_of_interest_by_companies_house_company_number

'''
    db = get_db()
    rows = query_db(db, sql_query)
    # rows = [(r.isoformat() if type(r) == datetime.datetime else r  for r in row) for row in rows]
    return {
        'headers': [
            'companiesHouseCompanyNumber',
            'countryOfInterest',
            'sectorSegmentOfInterest',
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
  datahub_company_id,
  country_of_interest_id,
  source,
  source_id,
  timestamp

from countries_of_interest_by_companies_house_company_number join 
  datahub_company_id_to_companies_house_company_number
  using (companies_house_company_number)

'''
    db = get_db()
    rows = query_db(db, sql_query)
    return {
        'headers': ['datahubCompanyID', 'countryOfInterest', 'source', 'sourceID', 'timestamp'],
        'data': [tuple(r) for r in rows]
    }

@app.route('/get-company-export-countries')
def get_company_export_countries():
    sql_query = '''
select
  datahub_company_id,
  export_country_id,
  source,
  source_id,
  timestamp

from export_countries_by_companies_house_company_number join 
  datahub_company_id_to_companies_house_company_number 
  using (companies_house_company_number)

'''
    db = get_db()
    rows = query_db(db, sql_query)
    return {
        'headers': ['datahubCompanyID', 'exportCountry', 'source', 'sourceID', 'timestamp'],
        'data': [tuple(r) for r in rows]
    }

@app.route('/get-datahub-company-ids')
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

@app.route('/get-sector-segments')
def get_sector_segments():
    sql_query = '''
select distinct
  segment

from segments

order by 1

'''
    db = get_db()
    rows = query_db(db, sql_query)
    return {
        'headers': ['sectorSegment'],
        'data': [r[0] for r in rows]
    }

if __name__ == '__main__':
    if cf_port is None:
        app.run(host='0.0.0.0', port=5000, debug=True)
    else:
        app.run(host='0.0.0.0', port=int(cf_port), debug=True)
        
