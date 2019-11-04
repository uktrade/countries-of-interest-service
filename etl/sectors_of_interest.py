from config import data_sources
from etl.etl import ETLTask

sql = '''
with omis_sectors_of_interest as (
    select
      company_id,
      sector,
      '{omis}' as source,
      id::varchar(100) as source_id,
      created_on as timestamp

    from omis

)

select * from omis_sectors_of_interest

'''.format(
    omis=data_sources.omis,
)

table_fields = '''(
    company_id varchar(100), 
    sector_of_interest varchar(200), 
    source varchar(50), 
    source_id varchar(100),
    timestamp timestamp,
    primary key (source, source_id)
)'''

table_name = 'coi_sectors_of_interest'

class Task(ETLTask):

    def __init__(self, sql=sql, table_fields=table_fields, table_name=table_name, *args, **kwargs):
        super().__init__(
            sql=sql,
            table_fields=table_fields,
            table_name=table_name,
            *args,
            **kwargs
        )
