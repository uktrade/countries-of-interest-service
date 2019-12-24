import celery

import app.application
import app.etl.tasks


@celery.task(ignore_result=True)
def populate_database_task(drop_table=True):
    with app.application.get_or_create().app_context():
        return app.etl.tasks.populate_database(drop_table)
