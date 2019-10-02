import os, sqlite3
from flask import Flask

cf_port = os.getenv("PORT")

app = Flask(__name__)

@app.route('/get-companies-affected-by-trade-barrier/<country>/<sector>')
def get_companies_affected_by_trade_barrier(country, sector):
    return 'get-companies-affected-by-trade-barrier, country: {country}, sector: {sector}'.format(country=country, sector=sector)

@app.route('/get-company-export-countries')
def get_company_export_countries():
    return 'get-company-export-countries'

@app.route('/get-company-countries-of-interest')
def get_company_countries_of_interest():
    return 'get-company-countries-of-interest'

if __name__ == '__main__':
    if cf_port is None:
        app.run(host='0.0.0.0', port=5000, debug=True)
    else:
        app.run(host='0.0.0.0', port=int(cf_port), debug=True)
        
