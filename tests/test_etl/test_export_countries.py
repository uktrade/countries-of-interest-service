from unittest.mock import Mock, patch
from tests.TestCase import TestCase
from app import app
from db import get_db
from etl.export_countries import Task
from utils.sql import query_database

class TestExportCountries(TestCase):

    def test(self):
        with app.app_context():
            with get_db() as connection:
                with connection.cursor() as cursor:
                    sql = 'create table datahub_export_countries ' \
                        '(company_id uuid, country varchar(2), id int)'
                    cursor.execute(sql)
                    values = [
                        ('a4881825-6c7c-46f3-b638-6a1346274a6b', 'CN', '0'),
                        ('f89d85d2-78c7-484d-bf63-228f32bf8d26', 'UK', '1'),
                    ]
                    sql = 'insert into datahub_export_countries values (%s, %s, %s)'
                    cursor.executemany(sql, values)


        with app.app_context():
            with get_db() as connection:
                task = Task(connection=connection)
                task()
                
                sql = ''' select * from coi_export_countries '''
                df = query_database(connection, sql)

        self.assertEqual(len(df), 2)
        self.assertEqual(df['company_id'].values[0], 'a4881825-6c7c-46f3-b638-6a1346274a6b')
        self.assertEqual(df['export_country'].values[0], 'CN')
        self.assertEqual(df['source'].values[0], 'datahub_export_countries')
        self.assertEqual(df['source_id'].values[0], '0')
        self.assertEqual(df['timestamp'].values[0], None)
        self.assertEqual(df['company_id'].values[1], 'f89d85d2-78c7-484d-bf63-228f32bf8d26')
        self.assertEqual(df['export_country'].values[1], 'UK')
        self.assertEqual(df['source'].values[1], 'datahub_export_countries')
        self.assertEqual(df['source_id'].values[1], '1')
        self.assertEqual(df['timestamp'].values[1], None)
