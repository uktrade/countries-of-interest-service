from app.config import constants
from app.db.models.external import (
    DatahubExportCountryHistory,
    DatahubExportToCountries,
    DITCountryTerritoryRegister,
)
from app.db.models.internal import (
    CountriesAndSectorsInterestTemp,
    StandardisedCountries,
)
from app.etl import ETLTask

sql = f'''
with export_country_history as (
    select distinct
        company_id::text as service_company_id,
        null::int4 as company_match_id,
        case
            when c.name is not null then c.name
            when s.standardised_country is not null then s.standardised_country
            else NULL
        end as country,
        null as sector,
        history_type || '_' || status as type,
        '{constants.Service.DATAHUB.value}' as service,
        '{constants.Source.DATAHUB_EXPORT_COUNTRY_HISTORY.value}' as source,
        d.history_id::text as source_id,
        history_date::timestamp as timestamp

    from {DatahubExportCountryHistory.get_fq_table_name()} d
        left join {DITCountryTerritoryRegister.get_fq_table_name()} c
            on d.country_iso_alpha2_code = c.country_iso_alpha2_code
        left join {StandardisedCountries.get_fq_table_name()} s
            on d.country = s.country and similarity > 90

    where status = 'currently_exporting'

    order by source, source_id

), export_to_countries as (
    select distinct
        company_id::text as service_company_id,
        null::int4 as company_match_id,
        case
            when c.name is not null then c.name
            when s.standardised_country is not null then s.standardised_country
            else NULL
        end as country,
        null as sector,
        '{constants.Type.EXPORTED.value}' as type,
        '{constants.Service.DATAHUB.value}' as service,
        '{constants.Source.DATAHUB_EXPORT_TO_COUNTRIES.value}' as source,
        d.id::text as source_id,
        null::timestamp as timestamp
    from {DatahubExportToCountries.get_fq_table_name()} d
        left join {DITCountryTerritoryRegister.get_fq_table_name()} c
            on d.country_iso_alpha2_code = c.country_iso_alpha2_code
        left join {StandardisedCountries.get_fq_table_name()} s
            on d.country = s.country and s.similarity > 90
    order by source, source_id

), results as (
    select * from export_country_history

    union all

    select * from export_to_countries
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

    name = constants.Task.EXPORT_COUNTRIES.value

    def __init__(self, sql=sql, model=CountriesAndSectorsInterestTemp, *args, **kwargs):
        super().__init__(
            sql=sql, model=model, *args, **kwargs,
        )
