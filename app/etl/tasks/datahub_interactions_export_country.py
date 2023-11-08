from app.config import constants
from app.db.models.external import DITCountryTerritoryRegister, InteractionsExportCountry
from app.db.models.internal import (
    CountriesAndSectorsInterestTemp,
    StandardisedCountries,
)
from app.etl import ETLTask


sql = f'''
with results as (
    select
        datahub_company_id as service_company_id,
        null::int4 as company_match_id,
        case
            when c.name is not null then c.name
            when s.standardised_country is not null then s.standardised_country
            else NULL
        end as country,
        null as sector,
        case
            when status = 'future_interest' then '{constants.Type.INTERESTED.value}'
            when status = 'currently_exporting'
                then '{constants.Type.EXPORTED.value}'
            else NULL
        end as type,
        '{constants.Service.DATAHUB.value}' as service,
        '{constants.Source.DATAHUB_INTERACTIONS_EXPORT_COUNTRY.value}' as source,
        datahub_interaction_export_country_id as source_id,
        created_on as timestamp

    from {InteractionsExportCountry.get_fq_table_name()} d
        left join {DITCountryTerritoryRegister.get_fq_table_name()} c
            on d.country_iso_alpha2_code = c.country_iso_alpha2_code
        left join {StandardisedCountries.get_fq_table_name()} s
            on d.country_name = s.country and similarity > 90

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
    name = constants.Task.INTERACTIONS_EXPORT_COUNTRY.value

    def __init__(self, sql=sql, model=CountriesAndSectorsInterestTemp, *args, **kwargs):
        super().__init__(
            sql=sql,
            model=model,
            *args,
            **kwargs,
        )
