import datetime
import numpy as np
from unittest.mock import Mock, patch
from tests.TestCase import TestCase
from app import app
from db import get_db
from etl.countries_and_sectors_of_interest import Task
from utils.sql import query_database

class TestCountriesAndSectorsOfInterest(TestCase):

    def test(self):
        with app.app_context():
            with get_db() as connection:
                with connection.cursor() as cursor:
                    sql = 'create table omis ' \
                        '''(
                             company_id uuid, 
                             country varchar(2), 
                             sector varchar(100), 
                             created_on timestamp, 
                             id uuid
                        )'''
                    cursor.execute(sql)
                    values = [
                        (
                            'a4881825-6c7c-46f3-b638-6a1346274a6b',
                            'CN',
                            'Food',
                            '2019-01-01 01:00',
                            '1ee5a16b-1a4b-4c84-838f-0d043579c9ba',
                        ),
                        (
                            'f89d85d2-78c7-484d-bf63-228f32bf8d26',
                            'UK',
                            'Engineering',
                            '2019-02-02 02:00',
                            'c0794724-c070-4c7e-a52c-89c0006bf7e6',
                        ),
                    ]
                    sql = 'insert into omis values (%s, %s, %s, %s, %s)'
                    cursor.executemany(sql, values)


        with app.app_context():
            with get_db() as connection:
                task = Task(connection=connection)
                task()
                
                sql = ''' select * from coi_countries_and_sectors_of_interest '''
                df = query_database(connection, sql)

        self.assertEqual(len(df), 2)
        self.assertEqual(df['company_id'].values[0], 'a4881825-6c7c-46f3-b638-6a1346274a6b')
        self.assertEqual(df['country_of_interest'].values[0], 'CN')
        self.assertEqual(df['sector_of_interest'].values[0], 'Food')
        self.assertEqual(df['source'].values[0], 'omis')
        self.assertEqual(df['source_id'].values[0], '1ee5a16b-1a4b-4c84-838f-0d043579c9ba')
        self.assertEqual(df['timestamp'].values[0], np.datetime64('2019-01-01 01:00'))
        self.assertEqual(df['company_id'].values[1], 'f89d85d2-78c7-484d-bf63-228f32bf8d26')
        self.assertEqual(df['country_of_interest'].values[1], 'UK')
        self.assertEqual(df['sector_of_interest'].values[1], 'Engineering')
        self.assertEqual(df['source'].values[1], 'omis')
        self.assertEqual(df['source_id'].values[1], 'c0794724-c070-4c7e-a52c-89c0006bf7e6')
        self.assertEqual(df['timestamp'].values[1], np.datetime64('2019-02-02 02:00'))

        sql = '''
        select
          tablename,
          indexname,
          indexdef
        
        FROM pg_indexes

        WHERE schemaname = 'public' 
          and tablename = 'coi_countries_and_sectors_of_interest' 
        '''
        df = query_database(connection, sql)
        self.assertTrue(len(df) >= 1) # index created

