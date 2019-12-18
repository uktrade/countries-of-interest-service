from app.config import data_sources
from app.etl import ETLTask

index = ('company_id',)

sql = '''
    select
      company_id::text,
      country,
      sector,
      '{}' as source,
      id::text as source_id,
      created_date as timestamp
    from datahub_omis
    order by 1
'''.format(
    data_sources.omis
)

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

    name = 'countries_and_sectors_of_interest'
    
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
