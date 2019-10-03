import os, sqlite3
from db import get_db, query_db
from flask import Flask

cf_port = os.getenv("PORT")

app = Flask(__name__)
if app.config['ENV'] == 'production':
    app.config['DATABASE'] = os.environ['DATABASE_URL']
else:
    app.config['DATABASE'] = 'postgresql://countries_of_interest_service@localhost'\
        '/countries_of_interest_service'

@app.route('/get-companies-affected-by-trade-barrier/<country>/<sector>')
def get_companies_affected_by_trade_barrier(country, sector):
    sql_query = '''
select
  companies_house_company_number
  
from countries_and_sectors_of_interest_by_companies_house_company_number

where country_of_interest_id = '{country}'
  and sector_of_interest_id = '{sector}'

'''.format(country=country, sector=sector)
    db = get_db()
    rows = query_db(db, sql_query)
    return {
        'headers': ['companyID'],
        'data': [r[0] for r in rows]
    }

@app.route('/get-company-export-countries')
def get_company_export_countries():
    sql_query = '''
select
  datahub_company_id,
  export_country_id

from export_countries_by_companies_house_company_number join 
  datahub_company_id_to_companies_house_company_number 
  using (companies_house_company_number)

'''
    db = get_db()
    rows = query_db(db, sql_query)
    return {
        'headers': ['datahubCompanyID', 'exportCountry'],
        'data': [tuple(r) for r in rows]
    }

@app.route('/get-company-countries-of-interest')
def get_company_countries_of_interest():
    sql_query = '''
select
  datahub_company_id,
  country_of_interest_id

from countries_of_interest_by_companies_house_company_number join 
  datahub_company_id_to_companies_house_company_number
  using (companies_house_company_number)

'''
    db = get_db()
    rows = query_db(db, sql_query)
    return {
        'headers': ['datahubCompanyID', 'countryOfInterest'],
        'data': [tuple(r) for r in rows]
    }

if __name__ == '__main__':
    if cf_port is None:
        app.run(host='0.0.0.0', port=5000, debug=True)
    else:
        app.run(host='0.0.0.0', port=int(cf_port), debug=True)
        
