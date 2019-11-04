from config import data_sources
from etl.etl import ETLTask

sql = '''
select
  company_id,
  country,
  l.sector,
  '{}' as source,
  l.id as source_id,
  l.created_on as timestamp

from omis l

order by 1

'''.format(data_sources.omis)

table_fields = '''(
    company_id varchar(100), 
    country_of_interest varchar(12), 
    sector_of_interest varchar(50), 
    source varchar(50), 
    source_id varchar(100),
    timestamp timestamp,
    primary key (source, source_id)
)'''

table_name = 'coi_countries_and_sectors_of_interest'

class Task(ETLTask):

    def __init__(self, sql=sql, table_fields=table_fields, table_name=table_name, *args, **kwargs):
        super().__init__(
            sql=sql,
            table_fields=table_fields,
            table_name=table_name,
            *args,
            **kwargs
        )
