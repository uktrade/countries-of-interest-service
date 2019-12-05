from config import data_sources
from etl import ETLTask


index = ('company_id',)

sql = '''
select 
  company_id,
  country,
  '{export_countries}' as source,
  id::varchar(100) as source_id,
  null::timestamp as timestamp
  
from datahub_export_countries

order by 1

'''.format(
    export_countries=data_sources.datahub_export_countries
)

table_fields = '''(
  company_id varchar(100), 
  export_country varchar(12), 
  source varchar(50),
  source_id varchar(100), 
  timestamp Timestamp,
  primary key (source, source_id)
)'''

table_name = 'coi_export_countries'


class Task(ETLTask):
    def __init__(
        self, sql=sql, table_fields=table_fields, table_name=table_name, *args, **kwargs
    ):
        super().__init__(
            index=index,
            sql=sql,
            table_fields=table_fields,
            table_name=table_name,
            *args,
            **kwargs
        )
