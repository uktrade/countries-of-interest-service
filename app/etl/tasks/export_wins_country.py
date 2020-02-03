from app.config import constants
from app.db.models.external import (
    DITCountryTerritoryRegister,
    ExportWins,
)
from app.db.models.internal import (
    CountriesAndSectorsInterestTemp,
    StandardisedCountries,
)
from app.etl import ETLTask


sql = f'''
with results as (
    select
        d.export_wins_company_id::text as service_company_id,
        null::int4 as company_match_id,
        case
            when c.name is not null then c.name
            when s.standardised_country is not null then s.standardised_country
            else NULL
        end as country,
        d.sector as sector,
        '{constants.Type.EXPORTED.value}' as type,
        '{constants.Service.EXPORT_WINS.value}' as service,
        '{constants.Source.EXPORT_WINS.value}' as source,
        d.export_wins_id::text as source_id,
        d.date_won::timestamp as timestamp
    from {ExportWins.get_fq_table_name()} d
        left join {DITCountryTerritoryRegister.get_fq_table_name()} c
            on d.country = c.country_iso_alpha2_code
        left join {StandardisedCountries.get_fq_table_name()} s
            on d.country = s.country and similarity > 90
    order by source, source_id
)

insert into {CountriesAndSectorsInterestTemp.get_fq_table_name()} (
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

    name = 'export_wins'

    def __init__(self, sql=sql, model=CountriesAndSectorsInterestTemp, *args, **kwargs):
        super().__init__(
            sql=sql, model=model, *args, **kwargs,
        )
