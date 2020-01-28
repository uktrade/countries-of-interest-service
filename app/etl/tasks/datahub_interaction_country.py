import app.db.models.external as external_models
import app.db.models.internal as internal_models
from app.config import data_sources
from app.etl import ETLTask

sql = f'''
select
    datahub_company_id::text as service_company_id,
    null as company_match_id,
    standardized_place::text as country,
    null as sector,
    'mentioned' as type,
    'datahub' as service,
    '{data_sources.datahub_interactions}' as source,
    datahub_interaction_id as source_id,
    created_on::timestamp as timestamp
from {external_models.Interactions.get_fq_table_name()}
    join {internal_models.InteractionsAnalysed.get_fq_table_name()}
    using (datahub_interaction_id)
order by source, source_id
'''


class Task(ETLTask):

    name = "mentioned_in_interactions"

    def __init__(
        self,
        sql=sql,
        model=internal_models.CountriesAndSectorsInterest,
        *args,
        **kwargs,
    ):
        super().__init__(
            sql=sql, model=model, *args, **kwargs,
        )
