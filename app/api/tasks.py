import celery

import app.application
import app.etl.tasks.core


@celery.task(ignore_result=True)
def populate_database_task(drop_table=True):
    with app.application.app.app_context():
        return app.etl.tasks.core.populate_database(drop_table)
