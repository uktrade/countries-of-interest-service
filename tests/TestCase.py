import psycopg2, unittest
import app
from db import get_db


def kill_connections(cursor):
    sql = '''
select pg_terminate_backend(pid) 

from pg_stat_activity 

where pid<> pg_backend_pid() 
    and datname = (select current_database());
'''
    cursor.execute(sql)

def flush_database(database_uri):
    with psycopg2.connect(database_uri) as connection:
        connection.autocommit = True
        with connection.cursor() as cursor:
            sql = 'select current_user'
            cursor.execute(sql)
            current_user = cursor.fetchone()[0]
            sql = 'DROP SCHEMA public CASCADE;'
            cursor.execute(sql)
            sql = 'CREATE SCHEMA public;'
            cursor.execute(sql)
            sql = 'GRANT ALL ON SCHEMA public TO {};'.format(current_user)
            cursor.execute(sql)
            sql = 'GRANT ALL ON SCHEMA public TO {};'.format(current_user)
            cursor.execute(sql)
    
class TestCase(unittest.TestCase):

    def setUp(self):
        self.config = app.app.config
        if self.config['ENV'] != 'test':
            raise Exception('run tests in test environment')
        self.client = app.app.test_client()
        self.database_uri = app.app.config['DATABASE']
        self.database_name = '/'.split(self.database_uri)[-1]
        
    def tearDown(self):
        # with psycopg2.connect(self.database_uri) as connection:
        #     connection.autocommit = True
        #     with connection.cursor() as cursor:
        #         kill_connections(cursor)
        flush_database(self.database_uri)
