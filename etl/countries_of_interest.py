from etl.etl import ETLTask

sql = '''
with omis_countries_of_interest as (
    select distinct
      company_number as companies_house_company_number,
      z.iso_alpha2_code as country_id,
      'datahub_order' as source,
      l.created_on as timestamp

    from order_order l join company_company r on l.company_id=r.id
      join metadata_country z on l.primary_market_id = z.id
    
    where company_number is not null 
      and company_number != ''
      and primary_market_id is not null

), datahub_countries_of_interest as (
    select distinct
      company_number as companies_house_company_number,
      z.iso_alpha2_code as country_id,
      'datahub_future_interest' as source,
      null::timestamp as timestamp

    from company_company_future_interest_countries l join company_company r on l.company_id=r.id
      join metadata_country z on l.country_id = z.id
    
    where company_number is not null and company_number != ''
    
), combined_countries_of_interest as (
  select * from omis_countries_of_interest
  
  union
  
  select * from datahub_countries_of_interest
  
)

select distinct
  companies_house_company_number,
  country_id,
  source,
  timestamp
  
from combined_countries_of_interest

where country_id is not null
  and country_id != ''

order by 1

'''

table_fields = '''(
    companies_house_company_number varchar(12), 
    country_of_interest_id varchar(12), 
    source varchar(50), 
    timestamp timestamp,
    primary key (companies_house_company_number, country_of_interest_id, source)
)'''

table_name = 'countries_of_interest_by_companies_house_company_number'

class Task(ETLTask):

    def __init__(self, sql=sql, table_fields=table_fields, table_name=table_name, *args, **kwargs):
        super().__init__(
            sql=sql,
            table_fields=table_fields,
            table_name=table_name,
            *args,
            **kwargs
        )
