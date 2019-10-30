import psycopg2, unittest
import app
from db import get_db


class TestCase(unittest.TestCase):

    def setUp(self):
        self.config = app.app.config
        self.client = app.app.test_client()
        connection = psycopg2.connect('postgresql://postgres@localhost')
        connection.autocommit = True
        cursor = connection.cursor()
        try:
            cursor.execute(''' create database test_countries_of_interest_service; ''')
            cursor.execute(''' create user test_countries_of_interest_service; ''')
            cursor.execute(''' grant all privileges on database ''' \
                           ''' test_countries_of_interest_service ''' \
                           ''' to test_countries_of_interest_service; '''
            )
        except:
            cursor.execute(''' drop database test_countries_of_interest_service; ''')
            cursor.execute(''' drop user test_countries_of_interest_service; ''')
            
        with app.app.app_context():
            db = get_db()

    def tearDown(self):
        connection = psycopg2.connect('postgresql://postgres@localhost')
        connection.autocommit = True
        cursor = connection.cursor()
        cursor.execute(''' drop database test_countries_of_interest_service; ''')
        cursor.execute( ''' drop user test_countries_of_interest_service; ''')
