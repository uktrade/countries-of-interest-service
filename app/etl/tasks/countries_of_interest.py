from app.config import data_sources
from app.db.models.external import DITCountryTerritoryRegister
from app.db.models.internal import StandardisedCountries
from app.etl import ETLTask

index = ('company_id',)

sql = '''
with omis_countries_of_interest as (
  select
    company_id::text,
    market as country_of_interest,
    case
      when c.name is not null then c.name
      when s.standardised_country is not null then s.standardised_country
      else NULL
    end as standardised_country,
    '{omis}' as source,
    d.id::text as source_id,
    created_date as timestamp

  from datahub_omis d
    left join {countries_and_territories_register} c
      on market = c.country_iso_alpha2_code
    left join {standardised_countries} s
      on market = s.country
        and similarity > 90

), datahub_countries_of_interest as (
  select
    company_id::text,
    case
      when d.country_iso_alpha2_code is not null
        and d.country_iso_alpha2_code  != '' then d.country_iso_alpha2_code
      else d.country
    end as country_of_interest,
    case
      when c.name is not null then c.name
      when s.standardised_country is not null then s.standardised_country
      else NULL
    end as standardised_country,
    '{future_interest}' as source,
    d.id::text as source_id,
    null::timestamp as timestamp

  from datahub_future_interest_countries d
    left join {countries_and_territories_register} c
      on d.country_iso_alpha2_code = c.country_iso_alpha2_code
    left join {standardised_countries} s
      on d.country = s.country
        and similarity > 90

), combined_countries_of_interest as (
  select * from omis_countries_of_interest

  union

  select * from datahub_countries_of_interest

), results as (

  select * from combined_countries_of_interest order by source, source_id

)

select * from results

'''.format(
    omis=data_sources.omis,
    future_interest=data_sources.datahub_future_interest_countries,
    countries_and_territories_register=DITCountryTerritoryRegister.__tablename__,
    standardised_countries=StandardisedCountries.__tablename__,
)

table_fields = '''(
    company_id text,
    country_of_interest text,
    standardised_country text,
    source text,
    source_id text,
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
