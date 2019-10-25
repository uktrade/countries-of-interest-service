from etl.config import (
    datahub_company_table_name,
    datahub_export_countries_table_name,
    export_countries_table_name,
)
from etl.etl import ETLTask

sql = '''
select distinct
  company_number as companies_house_company_number,
  country,
  'datahub_export_country' as source,
  l.id::varchar(100) as source_id,
  null::timestamp as timestamp
  
from {datahub_export_countries} l join {datahub_company} r on l.company_id = r.id

where company_number is not null
  and company_number != ''
  and country is not null
  and country != ''

order by 1

'''.format(
    datahub_export_countries=datahub_export_countries_table_name,
    datahub_company=datahub_company_table_name,
)

table_fields = '''(
  companies_house_company_number varchar(12) not null, 
  export_country varchar(12) not null, 
  source varchar(50) not null,
  source_id varchar(100), 
  timestamp Timestamp,
  primary key (
    companies_house_company_number, 
    export_country, 
    source,
    source_id
  )
)'''

table_name = export_countries_table_name

class Task(ETLTask):

    def __init__(self, sql=sql, table_fields=table_fields, table_name=table_name, *args, **kwargs):
        super().__init__(
            sql=sql,
            table_fields=table_fields,
            table_name=table_name,
            *args,
            **kwargs
        )
