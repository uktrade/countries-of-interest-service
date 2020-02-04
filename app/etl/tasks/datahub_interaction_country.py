from app.config import constants
from app.db.models.external import Interactions
from app.db.models.internal import (
    CountriesAndSectorsInterestTemp,
    InteractionsAnalysed,
)
from app.etl import ETLTask


sql = f'''
with results as (
    select distinct
        datahub_company_id::text as service_company_id,
        null::int4 as company_match_id,
        standardized_place::text as country,
        null as sector,
        '{constants.Type.MENTIONED.value}' as type,
        '{constants.Service.DATAHUB.value}' as service,
        '{constants.Source.DATAHUB_INTERACTIONS.value}' as source,
        datahub_interaction_id as source_id,
        created_on::timestamp as timestamp
    from {Interactions.get_fq_table_name()}
        join {InteractionsAnalysed.get_fq_table_name()}
        using (datahub_interaction_id)
    where standardized_place is not null
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

    name = constants.Task.MENTIONED_IN_INTERACTIONS.value

    def __init__(self, sql=sql, model=CountriesAndSectorsInterestTemp, *args, **kwargs):
        super().__init__(
            sql=sql, model=model, *args, **kwargs,
        )
