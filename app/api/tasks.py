import etl.tasks.core

import celery
import app.application


@celery.task(ignore_result=True)
def populate_database_task(drop_table=True):
    with app.application.app.app_context():
        return etl.tasks.core.populate_database(drop_table)
