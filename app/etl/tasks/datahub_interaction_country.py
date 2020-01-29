from app.config import data_sources
from app.db.models.external import Interactions
from app.db.models.internal import (
    CountriesAndSectorsInterest,
    InteractionsAnalysed,
)
from app.etl import ETLTask


sql = f'''
with results as (
    select
        datahub_company_id::text as service_company_id,
        null::int4 as company_match_id,
        standardized_place::text as country,
        null as sector,
        'mentioned' as type,
        'datahub' as service,
        '{data_sources.datahub_interactions}' as source,
        datahub_interaction_id as source_id,
        created_on::timestamp as timestamp
    from {Interactions.get_fq_table_name()}
        join {InteractionsAnalysed.get_fq_table_name()}
        using (datahub_interaction_id)
    where standardized_place is not null
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

    name = "mentioned_in_interactions"

    def __init__(self, sql=sql, model=CountriesAndSectorsInterest, *args, **kwargs):
        super().__init__(
            sql=sql, model=model, *args, **kwargs,
        )
