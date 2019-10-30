import sqlite3
import psycopg2
from flask import current_app, g

def close_db(e=None):
    db = g.pop('db', None)

    if db is not None:
        db.close()

def get_db():
    if 'db' not in g:
        g.db = psycopg2.connect(current_app.config['DATABASE'])
    return g.db
