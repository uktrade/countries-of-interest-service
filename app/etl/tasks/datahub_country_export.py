from app.config import data_sources
from app.db.models.external import DITCountryTerritoryRegister
from app.db.models.internal import CountriesAndSectorsInterest, StandardisedCountries
from app.etl import ETLTask

sql = f'''
select
    company_id::text as service_company_id,
    null as company_match_id,
    case
        when c.name is not null then c.name
        when s.standardised_country is not null then s.standardised_country
        else NULL
    end as country,
    null as sector,
    'exported' as type,
    'datahub' as service,
    '{data_sources.datahub_export_countries}' as source,
    d.id::text as source_id,
    null::timestamp as timestamp
from datahub_export_countries d
    left join {DITCountryTerritoryRegister.get_fq_table_name()} c
        on d.country_iso_alpha2_code = c.country_iso_alpha2_code
    left join {StandardisedCountries.get_fq_table_name()} s
        on d.country = s.country and s.similarity > 90
order by source, source_id
'''


class Task(ETLTask):

    name = 'export_countries'

    def __init__(self, sql=sql, model=CountriesAndSectorsInterest, *args, **kwargs):
        super().__init__(
            sql=sql, model=model, *args, **kwargs,
        )
