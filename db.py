from flask import current_app, g

import psycopg2


def close_db(e=None):
    db = g.pop('db', None)

    if db is not None:
        db.close()


def get_db():
    if 'db' not in g:
        g.db = psycopg2.connect(current_app.config['app']['database_url'])
    return g.db
