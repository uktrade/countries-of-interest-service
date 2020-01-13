import celery

import app.application
import app.etl.tasks


@celery.task(ignore_result=True)
def populate_database_task(drop_table=True, extractors=None, tasks=None):
    with app.application.get_or_create().app_context():
        extractors = extractors or []
        tasks = tasks or []
        return app.etl.tasks.populate_database(drop_table, extractors, tasks)
