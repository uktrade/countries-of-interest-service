from etl.etl import ETLTask
from etl.config import (
    datahub_company_table_name,
    datahub_future_interest_countries_table_name,
    omis_table_name,
    countries_of_interest_table_name
)

sql = '''
with omis_countries_of_interest as (
    select distinct
      company_number as companies_house_company_number,
      country,
      'datahub_omis' as source,
      l.id::varchar(100) as source_id,
      l.created_on as timestamp

    from {omis} l join {datahub_company} r on l.company_id=r.id
    
    where company_number is not null 
      and company_number != ''
      and country is not null

), datahub_countries_of_interest as (
    select distinct
      company_number as companies_house_company_number,
      country,
      'datahub_future_interest' as source,
      l.id::varchar(100) as source_id,
      null::timestamp as timestamp

    from {future_interest_countries} l join {datahub_company} r on l.company_id=r.id
    
    where company_number is not null and company_number != ''
    
), combined_countries_of_interest as (
  select * from omis_countries_of_interest
  
  union
  
  select * from datahub_countries_of_interest
  
)

select
  companies_house_company_number,
  country,
  source,
  source_id,
  timestamp
  
from combined_countries_of_interest

where country is not null
  and country != ''

order by 1

'''.format(
    datahub_company=datahub_company_table_name,
    future_interest_countries=datahub_future_interest_countries_table_name,
    omis=omis_table_name
)

table_fields = '''(
    companies_house_company_number varchar(12), 
    country_of_interest varchar(2), 
    source varchar(50), 
    source_id varchar(100),
    timestamp timestamp,
    primary key (companies_house_company_number, country_of_interest, source, source_id)
)'''

table_name = countries_of_interest_table_name

class Task(ETLTask):

    def __init__(self, sql=sql, table_fields=table_fields, table_name=table_name, *args, **kwargs):
        super().__init__(
            sql=sql,
            table_fields=table_fields,
            table_name=table_name,
            *args,
            **kwargs
        )
