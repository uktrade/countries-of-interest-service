import app.db.models.external as models
from app.config import data_sources
from app.etl import ETLTask

index = ('company_id',)

sql = '''
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
            '{source}' as source,
            o.id::text as source_id,
            created_date as timestamp

        from datahub_omis o
            left join {countries_and_territories_register} c
                on o.market::text = c.id
            left join {standardised_countries} s
                on o.market::text = s.country
                  and similarity > 90

        order by source_id

    )

    select * from results

'''.format(
    source=data_sources.omis,
    countries_and_territories_register=models.DITCountryTerritoryRegister.__tablename__,
    standardised_countries=models.StandardisedCountries.__tablename__,
)

table_fields = '''(
    company_id varchar(100),
    country_of_interest varchar(100),
    standardised_country varchar(100),
    sector_of_interest varchar(50),
    source varchar(50),
    source_id varchar(100),
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
            **kwargs
        )
