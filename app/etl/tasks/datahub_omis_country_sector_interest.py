from app.config import constants
from app.db.models.external import DatahubOmis, DITCountryTerritoryRegister
from app.db.models.internal import (
    CountriesAndSectorsInterestTemp,
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
        sector as sector,
        '{constants.Type.INTERESTED.value}' as type,
        '{constants.Service.DATAHUB.value}' as service,
        '{constants.Source.DATAHUB_OMIS.value}' as source,
        o.datahub_omis_order_id::text as source_id,
        created_date as timestamp
    from {DatahubOmis.get_fq_table_name()} o
        left join {DITCountryTerritoryRegister.get_fq_table_name()} c
            on o.market::text = c.country_iso_alpha2_code
        left join {StandardisedCountries.get_fq_table_name()} s
            on o.market::text = s.country and similarity > 90
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

    name = 'countries_and_sectors_of_interest'

    def __init__(self, sql=sql, model=CountriesAndSectorsInterestTemp, *args, **kwargs):
        super().__init__(
            sql=sql, model=model, *args, **kwargs,
        )
