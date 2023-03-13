import sqlalchemy.exc
from flask import current_app as flask_app


class ETLTask:
    name = ''

    def __init__(self, sql, model, drop_table=False):
        self.drop_table = drop_table
        self.sql = sql
        self.model = model

    def __call__(self, *args, **kwargs):
        with flask_app.db.engine.connect() as connection:
            with connection.begin() as transaction:
                try:
                    if self.drop_table is True:
                        self.model.drop_table()
                    self.model.create_table()

                    result = connection.execute(self.sql)
                    transaction.commit()
                    return {
                        'status': 200,
                        'rows': result.rowcount,
                        'task': self.name,
                    }
                except sqlalchemy.exc.ProgrammingError as err:
                    transaction.rollback()
                    flask_app.logger.error(f'Error running task, "{self.name}". Error: {err}')
                    raise err
                finally:
                    connection.close()
