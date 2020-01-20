from app.config import data_sources
from app.db.models.external import DITCountryTerritoryRegister
from app.db.models.internal import StandardisedCountries
from app.etl import ETLTask

index = ('company_id',)

sql = f'''
    with results as (
        select
            company_id::text,
            market as country_of_interest,
            case
                when c.name is not null then c.name
                when s.standardised_country is not null then s.standardised_country
                else NULL
            end as standardised_country,
            sector as sector_of_interest,
            '{data_sources.omis}' as source,
            o.datahub_omis_order_id::text as source_id,
            created_date as timestamp

        from datahub_omis o
            left join {DITCountryTerritoryRegister.get_fq_table_name()} c
                on o.market::text = c.country_iso_alpha2_code
            left join {StandardisedCountries.get_fq_table_name()} s
                on o.market::text = s.country
                  and similarity > 90

        order by source_id

    )

    select * from results

'''

table_fields = '''(
    company_id text,
    country_of_interest text,
    standardised_country text,
    sector_of_interest text,
    source text,
    source_id text,
    timestamp timestamp,
    primary key (source, source_id)
)'''

table_name = 'coi_countries_and_sectors_of_interest'


class Task(ETLTask):

    name = 'countries_and_sectors_of_interest'

    def __init__(
        self, sql=sql, table_fields=table_fields, table_name=table_name, *args, **kwargs
    ):
        super().__init__(
            index=index,
            sql=sql,
            table_fields=table_fields,
            table_name=table_name,
            *args,
            **kwargs,
        )
