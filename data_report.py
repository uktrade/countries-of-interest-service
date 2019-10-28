import psycopg2
from db import get_db
from utils.sql import query_database
from utils.utils import to_web_dict, to_camel_case

summary_table_sql = '''
with datahub_company_summary as (
  select
    count(1) as n_companies,
    count(sector) as n_companies_matched_to_sector

  from datahub_company

), matched_companies as (
    select
        id,
        company_number
        
    from datahub_company
    
    where company_number is not null
        and company_number not in ('', 'NotRegis', 'n/a', 'Not reg', 'N/A')

), duplicates as (
    select
        company_number,
        count(1)
    from matched_companies
    group by 1
    
    having count(1) > 1

), duplicate_summary as (
    select
        sum(count) as n_companies_matched_to_duplicate_companies_house
        
    from duplicates
    
), match_summary as (
    select
        count(1) as n_companies_matched_to_companies_house
        
    from matched_companies
    
), sector_summary as (
  select
    count(1) as n_sectors

  from datahub_sector

), omis_summary as (
  select
    count(distinct company_id) as n_companies_with_omis_orders

  from omis

), export_countries_summary as (
  select
    count(distinct company_id) as n_companies_with_export_countries

  from datahub_export_countries

), future_interest_countries_summary as (
  select
    count(distinct company_id) as n_companies_with_future_interest_countries

  from datahub_future_interest_countries

), results as (
  select 
    n_companies,
    n_companies_matched_to_companies_house,
    n_companies_matched_to_sector,
    coalesce(n_companies_matched_to_duplicate_companies_house, 0::float) as n_companies_matched_to_duplicate_companies_house,
    n_companies_with_export_countries,
    n_companies_with_future_interest_countries,
    n_companies_with_omis_orders,
    n_sectors

  from datahub_company_summary
    join match_summary on 1=1
    join duplicate_summary on 1=1
    join sector_summary on 1=1
    join omis_summary on 1=1
    join export_countries_summary on 1=1
    join future_interest_countries_summary on 1=1

)

select * from results

'''

top_sectors_sql = '''
with results as (
  select
    sector,
    count(1)

  from datahub_company

  group by 1

  order by 2 desc

)

select * from results

'''

omis_orders_sql = '''
with results as (
  select
    date_trunc('d', created_on) as date,
    count(1) as frequency

  from omis

  group by 1

  order by 1

)

select * from results
'''

def get_summary_table():
    connection = get_db()
    df = query_database(connection, summary_table_sql)
    return df

def get_top_sectors():
    connection = get_db()
    df = query_database(connection, top_sectors_sql)
    return df

def get_omis_order_frequency():
    connection = get_db()
    df = query_database(connection, omis_orders_sql)
    return df

def get_data_report_data():
    data = {}
    df_summary_table = get_summary_table()
    df_top_sectors = get_top_sectors()
    df_omis_orders = get_omis_order_frequency()
    for c in df_summary_table.columns:
        data[to_camel_case(c)] = df_summary_table[c].values[0]
    data['topSectors'] = to_web_dict(df_top_sectors)
    data['omisOrderFrequency'] = to_web_dict(df_omis_orders)
    return data

if __name__ == '__main__':
    def get_db():
        uri = 'postgres://countries_of_interest_service@localhost/countries_of_interest_service'
        return psycopg2.connect(uri)

    data = get_data_report_data()
    print(data)
        
