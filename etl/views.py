import etl.tasks
from flask import request


def populate_database():
    drop_table = 'drop-table' in request.args
    etl.tasks.populate_database.delay(drop_table)
    return {'status': 200}
