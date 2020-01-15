import logging

from flask import current_app

import sqlalchemy


import app.db.models.external as external_models
import app.db.models.internal as internal_models


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


index = ('company_id',)

sql = '''
with results as (
  select
    datahub_company_id::text as company_id,
    standardized_place::text as country_of_interest,
    datahub_interaction_id::text as interaction_id,
    created_on::timestamp as timestamp

  from {interactions} join {interactions_analysed} using (datahub_interaction_id)

)

insert into {mentioned_in_interactions} (
    company_id,
    country_of_interest,
    interaction_id,
    timestamp
) select * from results

'''.format(
    interactions=external_models.Interactions.__tablename__,
    interactions_analysed='{}.{}'.format(
        internal_models.InteractionsAnalysed.__table_args__['schema'],
        internal_models.InteractionsAnalysed.__tablename__,
    ),
    mentioned_in_interactions=internal_models.MentionedInInteractions.__tablename__,
)


class Task:

    name = "mentioned_in_interactions"
    table_name = internal_models.MentionedInInteractions.__tablename__

    def __init__(self, drop_table=True, *args, **kwargs):
        self.drop_table = drop_table

    def __call__(self, *args, **kwargs):
        connection = current_app.db.engine.connect()
        transaction = connection.begin()
        try:
            if self.drop_table is True:
                internal_models.MentionedInInteractions.__table__.delete()
            result = connection.execute(sql)
            transaction.commit()
            return {
                'status': 200,
                'rows': result.rowcount,
                'table': internal_models.MentionedInInteractions.__tablename__,
            }
        except sqlalchemy.exc.ProgrammingError as err:
            transaction.rollback()
            logger.error(f'Error running task, "{self.name}". Error: {err}')
            raise err
        finally:
            connection.close()
