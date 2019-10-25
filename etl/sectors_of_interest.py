from etl.config import (
    datahub_company_table_name,
    omis_table_name,
    sectors_of_interest_table_name,
)
from etl.etl import ETLTask

sql = '''
with omis_sectors_of_interest as (
    select distinct
      company_id,
      sector,
      'datahub_omis' as source,
      id::varchar(100) as source_id,
      created_on as timestamp

    from {omis}

    where sector is not null

), results as (
  select
    company_number as companies_house_company_number,
    omis_sectors_of_interest.sector as sector_of_interest,
    source,
    source_id,
    timestamp

  from {datahub_company} join omis_sectors_of_interest on 
      {datahub_company}.id = omis_sectors_of_interest.company_id    
  
  where company_number != ''
    and company_number is not null

)

select * from results order by 1, 5

'''.format(
    datahub_company=datahub_company_table_name,
    omis=omis_table_name,
)

table_fields = '''(
    companies_house_company_number varchar(12), 
    sector_of_interest varchar(200), 
    source varchar(50), 
    source_id varchar(100),
    timestamp timestamp,
    primary key (companies_house_company_number, sector_of_interest, source, source_id)
)'''

table_name = sectors_of_interest_table_name

class Task(ETLTask):

    def __init__(self, sql=sql, table_fields=table_fields, table_name=table_name, *args, **kwargs):
        super().__init__(
            sql=sql,
            table_fields=table_fields,
            table_name=table_name,
            *args,
            **kwargs
        )
