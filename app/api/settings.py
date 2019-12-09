import datetime

from flask import current_app as flask_app
from flask.json import JSONEncoder

import numpy as np

from db import get_db

from utils.sql import execute_query


class CustomJSONEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        elif isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return str(obj)


def create_users_table():
    with flask_app.app_context():
        if flask_app.config['flask']['env'] != 'test':
            users = [
                (
                    flask_app.config['dataflow']['hawk_client_id'],
                    flask_app.config['dataflow']['hawk_client_key'],
                ),
            ]
            sql = 'drop table if exists users'
            with get_db() as connection:
                execute_query(connection, sql)
            sql = (
                'create table users ('
                'client_id varchar(100) primary key,'
                'client_key varchar(200)'
                ')'
            )
            with get_db() as connection:
                execute_query(connection, sql)
            sql = 'insert into users values (%s, %s) '
            with get_db() as connection:
                cursor = connection.cursor()
                cursor.executemany(sql, users)
