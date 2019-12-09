from flask import current_app as flask_app

import etl.tasks.core

import celery


@celery.task
def populate_database_task(drop_table=True):
    return etl.tasks.core.populate_database(drop_table)
