from app.config import data_sources
from app.db.models.external import DITCountryTerritoryRegister
from app.db.models.internal import StandardisedCountries
from app.etl import ETLTask


index = ('company_id',)

sql = '''
select
  company_id::text,
  case
    when d.country_iso_alpha2_code is not null
      and d.country_iso_alpha2_code != '' then d.country_iso_alpha2_code
    else d.country
  end as export_country,
  case
    when c.name is not null then c.name
    when s.standardised_country is not null then s.standardised_country
    else NULL
  end as standardised_country,
  '{export_countries}' as source,
  d.id::text as source_id,
  null::timestamp as timestamp

from datahub_export_countries d
  left join {countries_and_territories_register} c
    on d.country_iso_alpha2_code = c.country_iso_alpha2_code
  left join {standardised_countries} s
    on d.country = s.country
      and s.similarity > 90

order by source, source_id

'''.format(
    export_countries=data_sources.datahub_export_countries,
    countries_and_territories_register=DITCountryTerritoryRegister.__tablename__,
    standardised_countries=StandardisedCountries.__tablename__,
)

table_fields = '''(
  company_id text,
  export_country text,
  standardised_country text,
  source text,
  source_id text,
  timestamp Timestamp,
  primary key (source, source_id)
)'''

table_name = 'coi_export_countries'


class Task(ETLTask):

    name = 'export_countries'

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
