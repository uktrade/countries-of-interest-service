from flask import current_app

import sqlalchemy


import app.db.models.external as external_models
import app.db.models.internal as internal_models
from app.config import data_sources

index = ('company_id',)

sql = '''
with results as (
  select
    datahub_company_id::text as company_id,
    standardized_place::text as country_of_interest,
    '{source}'::text as source,
    datahub_interaction_id::text as source_id,
    created_on::timestamp as timestamp

  from {interactions} join {interactions_analysed} using (id)

)

insert into {mentioned_in_interactions} select * from results

'''.format(
    source=data_sources.datahub_interactions,
    interactions=external_models.Interactions.__tablename__,
    interactions_analysed='{}.{}'.format(
        internal_models.InteractionsAnalysed.__table_args__['schema'],
        internal_models.InteractionsAnalysed.__tablename__,
    ),
    mentioned_in_interactions=internal_models.MentionedInInteractions.__tablename__,
)


class Task:

    name = "mentioned_in_interactions"

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
            raise err
            return {
                'status': 500,
                'error': str(err),
                'table': internal_models.MentionedInInteractions.__tablename__,
            }
        finally:
            connection.close()
        return {
            'status': 500,
            'table': internal_models.MentionedInInteractions.__tablename__,
        }
