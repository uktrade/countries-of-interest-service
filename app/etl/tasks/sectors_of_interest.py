from app.config import data_sources
from app.etl import ETLTask

index = ('company_id',)

sql = '''
with omis_sectors_of_interest as (
    select
      company_id::text,
      sector as sector_of_interest,
      '{omis}' as source,
      datahub_omis_order_id as source_id,
      created_date as timestamp

    from datahub_omis

)

select * from omis_sectors_of_interest

'''.format(
    omis=data_sources.omis,
)

table_fields = '''(
    company_id text,
    sector_of_interest text,
    source text,
    source_id text,
    timestamp timestamp,
    primary key (source, source_id)
)'''

table_name = 'coi_sectors_of_interest'


class Task(ETLTask):

    name = 'sectors_of_interest'

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
