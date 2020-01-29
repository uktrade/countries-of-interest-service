from app.config import data_sources
from app.db.models.external import (
    DatahubFutureInterestCountries,
    DITCountryTerritoryRegister,
)
from app.db.models.internal import (
    CountriesAndSectorsInterest,
    StandardisedCountries,
)
from app.etl import ETLTask

sql = f'''
with results as (
    select
        company_id::text as service_company_id,
        null::int4 as company_match_id,
        case
            when c.name is not null then c.name
            when s.standardised_country is not null then s.standardised_country
            else NULL
        end as country,
        null as sector,
        'interested' as type,
        'datahub' as service,
        '{data_sources.datahub_future_interest_countries}' as source,
        d.id::text as source_id,
        null::timestamp as timestamp
    from {DatahubFutureInterestCountries.get_fq_table_name()} d
        left join {DITCountryTerritoryRegister.get_fq_table_name()} c
            on d.country_iso_alpha2_code = c.country_iso_alpha2_code
        left join {StandardisedCountries.get_fq_table_name()} s
            on d.country = s.country and similarity > 90
    order by source, source_id
)

insert into {CountriesAndSectorsInterest.get_fq_table_name()} (
    service_company_id,
    company_match_id,
    country,
    sector,
    type,
    service,
    source,
    source_id,
    timestamp
) select * from results

'''


class Task(ETLTask):

    name = 'countries_of_interest'

    def __init__(self, sql=sql, model=CountriesAndSectorsInterest, *args, **kwargs):
        super().__init__(
            sql=sql, model=model, *args, **kwargs,
        )
