from etl.etl import ETLTask
from etl.config import datahub_company_table_name, countries_and_sectors_of_interest_table_name

sql = '''
select distinct
  company_number as companies_house_company_number,
  country,
  l.sector,
  'datahub_order' as source,
  l.id as source_id,
  l.created_on as timestamp

from omis l join {datahub_company_table_name} r on l.company_id=r.id

where company_number is not null
  and company_number != ''

order by 1

'''.format(
    datahub_company_table_name=datahub_company_table_name
)

table_fields = '''(
    companies_house_company_number varchar(12), 
    country_of_interest varchar(12), 
    sector_of_interest varchar(50), 
    source varchar(50), 
    source_id varchar(100),
    timestamp timestamp,
    primary key (companies_house_company_number, country_of_interest, source, source_id)
)'''

table_name = countries_and_sectors_of_interest_table_name

class Task(ETLTask):

    def __init__(self, sql=sql, table_fields=table_fields, table_name=table_name, *args, **kwargs):
        super().__init__(
            sql=sql,
            table_fields=table_fields,
            table_name=table_name,
            *args,
            **kwargs
        )
