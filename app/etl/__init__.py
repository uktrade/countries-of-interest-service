import logging

import sqlalchemy.exc
from flask import current_app

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class ETLTask:
    name = ''

    def __init__(self, sql, model, drop_table=False):
        self.drop_table = drop_table
        self.sql = sql
        self.model = model
        self.table_name = model.__tablename__

    def __call__(self, *args, **kwargs):
        connection = current_app.db.engine.connect()
        transaction = connection.begin()
        try:
            if self.drop_table is True:
                self.model.drop_table()
            self.model.create_table()

            result = connection.execute(self.sql)
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
