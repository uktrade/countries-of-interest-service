from app.config import data_sources
from app.etl import ETLTask

index = ('company_id',)

sql = '''
with omis_countries_of_interest as (
    select
      company_id::text,
      market,
      '{omis}' as source,
      id::varchar(100) as source_id,
      created_date as timestamp

    from datahub_omis

), datahub_countries_of_interest as (
    select
      company_id::text,
      country_iso_alpha2_code,
      '{future_interest}' as source,
      id::varchar(100) as source_id,
      null::timestamp as timestamp

    from datahub_future_interest_countries

), combined_countries_of_interest as (
  select * from omis_countries_of_interest

  union

  select * from datahub_countries_of_interest

)

select
  company_id::text,
  market as country_of_interest,
  source,
  source_id,
  timestamp

from combined_countries_of_interest

order by 1

'''.format(
    omis=data_sources.omis,
    future_interest=data_sources.datahub_future_interest_countries,
)

table_fields = '''(
    company_id varchar(100),
    country_of_interest varchar(2),
    source varchar(50),
    source_id varchar(100),
    timestamp timestamp,
    primary key (source, source_id)
)'''

table_name = 'coi_countries_of_interest'


class Task(ETLTask):

    name = 'countries_of_interest'

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
