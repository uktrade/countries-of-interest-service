from etl.etl import ETLTask

sql = '''
select distinct
  company_number as companies_house_company_number,
  primary_market_id as country_id,
  l.sector_id,
  concat('datahub_order:', l.id) as source,
  l.created_on as timestamp
  

from order_order l join company_company r on l.company_id=r.id

where company_number is not null
  and company_number != ''

order by 1

'''

table_fields = '''(
    companies_house_company_number varchar(12), 
    country_of_interest_id uuid, 
    sector_of_interest_id uuid, 
    source varchar(50), 
    timestamp timestamp,
    primary key (companies_house_company_number, country_of_interest_id, source)
)'''

table_name = 'countries_and_sectors_of_interest_by_companies_house_company_number'

class Task(ETLTask):

    def __init__(self, sql=sql, table_fields=table_fields, table_name=table_name, *args, **kwargs):
        super().__init__(
            sql=sql,
            table_fields=table_fields,
            table_name=table_name,
            *args,
            **kwargs
        )
