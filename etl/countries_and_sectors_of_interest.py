from etl.etl import ETLTask

sql = '''
select distinct
  company_number as companies_house_company_number,
  z.iso_alpha2_code as country_id,
  segment,
  'datahub_order' as source,
  l.created_on as timestamp
  

from order_order l join company_company r on l.company_id=r.id
  join metadata_country z on l.primary_market_id = z.id
  join metadata_sector y on l.sector_id = y.id

where company_number is not null
  and company_number != ''

order by 1

'''

table_fields = '''(
    companies_house_company_number varchar(12), 
    country_of_interest_id varchar(12), 
    sector_segment varchar(50), 
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
