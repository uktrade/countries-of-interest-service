import celery

import etl.tasks.core


@celery.task
def populate_database_task(drop_table=True):
    return etl.tasks.core.populate_database(drop_table)
