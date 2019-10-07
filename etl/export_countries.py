from etl.etl import ETLTask

sql = '''
select distinct
  company_number as companies_house_company_number,
  iso_alpha2_code as country_id,
  'datahub_export_country' as source
  
from company_company_export_to_countries l join company_company r on l.company_id = r.id
  join metadata_country z on l.country_id = z.id

where company_number is not null
  and company_number != ''
  and iso_alpha2_code is not null
  and iso_alpha2_code != ''

order by 1

'''

table_fields = '''(
  companies_house_company_number varchar(12) not null, 
  export_country_id varchar(12) not null, 
  source varchar(50) not null, 
  primary key (
    companies_house_company_number, 
    export_country_id, 
    source
  )
)'''

table_name = 'export_countries_by_companies_house_company_number'

class Task(ETLTask):

    def __init__(self, sql=sql, table_fields=table_fields, table_name=table_name, *args, **kwargs):
        super().__init__(
            sql=sql,
            table_fields=table_fields,
            table_name=table_name,
            *args,
            **kwargs
        )
