import unittest
from unittest.mock import Mock, patch
from etl import create_index, ETLTask
from tests.TestCase import TestCase
from utils.sql import query_database

from app import app
from db import get_db


@patch('etl.create_index')
@patch('etl.create_table')
@patch('etl.drop_table')
@patch('etl.insert_data')
@patch('etl.query_database')
class TestEtlTask(unittest.TestCase):
    def test_creates_index(
            self,
            query_database,
            insert_data,
            drop_table,
            create_table,
            create_index
    ):
        connection = Mock(name='connection')
        index = Mock(name='index')
        sql = Mock(name='sql')
        table_fields = Mock(name='table_fields')
        table_name = Mock(name='table_name')
        task = ETLTask(connection, sql, table_fields, table_name, index=index)
        task()
        create_index.assert_called_once_with(connection, index, table_name)

    def test_doesnt_create_index_if_no_index_argument(
            self,
            query_database,
            insert_data,
            drop_table,
            create_table,
            create_index
    ):
        connection = Mock(name='connection')
        sql = Mock(name='sql')
        table_fields = Mock(name='table_fields')
        table_name = Mock(name='table_name')
        task = ETLTask(connection, sql, table_fields, table_name)
        task()
        create_index.assert_not_called()


class TestCreateIndex(TestCase):
    def setUp(self):
        super().setUp()
        self.index = ['source', 'source_id']
        self.table_name = 'test_table'
        sql_create = (
            '''
            create table {}
                (source varchar(50), source_id varchar(100), val int)
            '''
        )
        sql_create = sql_create.format(self.table_name)
        with app.app_context():
            with get_db() as connection:
                with connection.cursor() as cursor:
                    cursor.execute(sql_create)

    def get_indices(self):
        with app.app_context():
            with get_db() as connection:
                sql = '''
                select
                    tablename,
                    indexname,
                    indexdef

                FROM pg_indexes

                WHERE schemaname = 'public' and tablename = %s'''
                return query_database(connection, sql, [self.table_name])

    def test(self):
        with app.app_context():
            with get_db() as connection:
                create_index(connection, self.index, self.table_name)
        df = self.get_indices()
        self.assertEqual(len(df), 1)
        self.assertEqual(df['tablename'].values[0], self.table_name)
        self.assertEqual(
            df['indexname'].values[0],
            '{}_index'.format(self.table_name)
        )

    def test_index_name_argument(self):
        index_name = 'index_name'
        with app.app_context():
            with get_db() as connection:
                create_index(
                    connection,
                    self.index,
                    self.table_name,
                    index_name=index_name
                )
        df = self.get_indices()
        self.assertEqual(df['indexname'].values[0], index_name)
