from flask import current_app as flask_app

import app.etl.tasks


def populate_database_task(drop_table=True, extractors=None, tasks=None):
    with flask_app.app_context():
        extractors = extractors or []
        tasks = tasks or []
        return app.etl.tasks.populate_database(drop_table, extractors, tasks)
