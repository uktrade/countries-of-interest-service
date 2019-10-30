import psycopg2, unittest
import app
from db import get_db


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

        connection.close()

    def tearDown(self):
        # self.connection.close()
        connection = psycopg2.connect('postgresql://postgres@localhost')
        connection.autocommit = True
        cursor = connection.cursor()
        cursor.execute('''drop database if exists {};'''.format(self.database_name))
        cursor.execute('''drop user if exists {};'''.format(self.user_name))
