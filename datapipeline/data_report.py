

# def get_data_report_data():
#     connection = get_db()
#     output = {}
#     sql = ''' 
# select 
#     n_companies, 
#     n_companies_with_orders 

# from companies_with_orders 

# order by timestamp desc 

# limit 1 '''
#     df = query_database(connection, sql)
#     web_dict = to_web_dict(df)
#     web_dict['data'] = web_dict['data'][0]
#     output['companyOrderSummary'] =  web_dict

#     sql = ''' 
# select 
#     n_companies, 
#     n_companies_with_export_countries

# from companies_with_export_countries

# order by timestamp desc 

# limit 1 '''
#     df = query_database(connection, sql)
#     web_dict = to_web_dict(df)
#     web_dict['data'] = web_dict['data'][0]
#     output['companyExportCountriesSummary'] = web_dict

#     sql = ''' 
# select 
#     n_companies, 
#     n_companies_with_countries_of_interest

# from companies_with_countries_of_interest

# order by timestamp desc 

# limit 1 '''
#     df = query_database(connection, sql)
#     web_dict = to_web_dict(df)
#     web_dict['data'] = web_dict['data'][0]
#     output['countriesOfInterestSummary'] = web_dict

#     order_frequencies = {}
#     sql = ''' 
# with latest_timestamp as (
#   select max(timestamp) as timestamp from order_frequency_by_date order by timestamp desc limit 1
# ), order_frequency as (
#   select * from order_frequency_by_date join latest_timestamp using (timestamp)
# )
# select 
#     date, 
#     count as n_orders

# from order_frequency

# order by date

# '''
#     df = query_database(connection, sql)
#     web_dict = to_web_dict(df)
#     order_frequencies['daily'] = web_dict

#     sql = ''' 
# with latest_timestamp as (
#   select max(timestamp) as timestamp from order_frequency_by_date order by timestamp desc limit 1
# ), order_frequency as (
#   select * from order_frequency_by_date join latest_timestamp using (timestamp)
# )
# select 
#     date_trunc('week', date) as date, 
#     sum(count) as n_orders

# from order_frequency

# group by 1

# order by date

# '''
#     df = query_database(connection, sql)
#     web_dict = to_web_dict(df)
#     order_frequencies['weekly'] = web_dict

#     sql = ''' 
# with latest_timestamp as (
#   select max(timestamp) as timestamp from order_frequency_by_date order by timestamp desc limit 1
# ), order_frequency as (
#   select * from order_frequency_by_date join latest_timestamp using (timestamp)
# )
# select 
#     date_trunc('month', date) as date, 
#     sum(count) as n_orders

# from order_frequency

# group by 1

# order by date

# '''
#     df = query_database(connection, sql)
#     web_dict = to_web_dict(df)
#     order_frequencies['monthly'] = web_dict

#     output['orderFrequency'] = order_frequencies
    
#     return output

# @app.route('/api/get-matched-companies')
# @hawk_required
# def get_matched_companies():
#     sql = ''' select * from matched_companies order by timestamp limit 1 '''
#     db = get_db()
#     rows = query_db(db, sql)
#     row = rows[0]
#     return {
#         'nCompanies': row[0],
#         'nMatches': row[1],
#         'nDuplicates': row[2],
#         'nUniqueMatches': row[3]
#     }

# @app.route('/api/get-sector-matches')
# @hawk_required
# def get_sector_matches():
#     sql = ''' select * from sector_matches order by timestamp limit 1 '''
#     db = get_db()
#     rows = query_db(db, sql)
#     row = rows[0]
#     return {
#         'nCompanies': row[0],
#         'nSectors': row[1],
#         'nMatches': row[2],
#     }

# @app.route('/api/get-top-sectors')
# @hawk_required
# def get_top_sectors():
#     sql = ''' with latest_top_sectors as (
#       select max(timestamp) as timestamp_most_recent from top_sectors

#     ), top_sectors_most_recent as (
#       select
#         sector, 
#         count

#       from top_sectors join latest_top_sectors on 1=1 

#       where timestamp = timestamp_most_recent
#     )
#     select * from top_sectors_most_recent order by count desc '''
#     db = get_db()
#     rows = query_db(db, sql)
#     return {
#         'data': [{'name': row[0], 'count': row[1]} for row in rows]
#     }
