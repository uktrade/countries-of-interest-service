import psycopg2, unittest
import app
from db import get_db

def kill_connections(cursor, database_name):
    sql = '''
select pg_terminate_backend(pid) 

from pg_stat_activity 

where pid<> pg_backend_pid() 
    and datname = %s;
'''
    cursor.execute(sql, [database_name])
    

class TestCase(unittest.TestCase):

    def setUp(self):
        self.config = app.app.config
        self.client = app.app.test_client()
        self.database_name = 'test_countries_of_interest_service'
        self.user_name = 'test_countries_of_interest_service'

        connection = psycopg2.connect('postgresql://postgres@localhost')
        connection.autocommit = True
        cursor = connection.cursor()

        sql = ''' drop database if exists {}; '''.format(self.database_name)
        cursor.execute(sql)
        sql = ''' drop user if exists {}; '''.format(self.user_name)
        cursor.execute(sql)
        
        try:
            cursor.execute(''' create user {}; '''.format(self.user_name))
            cursor.execute(''' create database {}; '''.format(self.database_name))
            cursor.execute(''' alter database {} owner to {} '''.format(
                self.database_name,
                self.user_name)
            )
            cursor.execute(''' grant all privileges on database ''' \
                           ''' {} ''' \
                           ''' to {}; '''.format(self.database_name, self.user_name)
            )
        except:
            cursor.execute(''' drop database {}; '''.format(self.database_name))
            cursor.execute(''' drop user {}; '''.format(self.user_name))
            raise Exception('failed to setup test')
        finally:
            connection.close()

    def tearDown(self):

        with psycopg2.connect('postgresql://postgres@localhost') as connection:
            connection.autocommit = True
            with connection.cursor() as cursor:
                kill_connections(cursor, self.database_name)
                cursor.execute('''drop database if exists {};'''.format(self.database_name))
                cursor.execute('''drop user if exists {};'''.format(self.user_name))
