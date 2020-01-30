import logging

import sqlalchemy.exc
from flask import current_app

import app.db.models.external as external_models
import app.db.models.internal as internal_models
from app.config import data_sources

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


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
    from {external_models.Interactions.get_fq_table_name()}
        join {internal_models.InteractionsAnalysed.get_fq_table_name()}
        using (datahub_interaction_id)
    order by source, source_id
)

insert into {internal_models.CountriesAndSectorsInterest.get_fq_table_name()} (
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


class Task:

    name = "mentioned_in_interactions"

    def __init__(self, drop_table=False, *args, **kwargs):
        self.drop_table = drop_table
        self.model = internal_models.CountriesAndSectorsInterest
        self.table_name = self.model.__tablename__

    def __call__(self, *args, **kwargs):
        connection = current_app.db.engine.connect()
        transaction = connection.begin()
        try:
            if self.drop_table is True:
                self.model.drop_table()
            self.model.create_table()

            result = connection.execute(sql)
            transaction.commit()
            return {
                'status': 200,
                'rows': result.rowcount,
                'table': self.table_name,
            }
        except sqlalchemy.exc.ProgrammingError as err:
            transaction.rollback()
            logger.error(f'Error running task, "{self.name}". Error: {err}')
            raise err
        finally:
            connection.close()
