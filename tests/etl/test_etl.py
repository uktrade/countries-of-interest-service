from unittest.mock import Mock, patch

import pytest

from app.db.db_utils import create_index, execute_query, execute_statement
from app.etl import ETLTask


@patch('app.etl.create_index')
@patch('app.etl.create_table')
@patch('app.etl.drop_table')
@patch('app.etl.insert_data')
@patch('app.etl.execute_query')
class TestEtlTask:
    def test_creates_index(
        self, execute_query, insert_data, drop_table, create_table, create_index
    ):
        index = Mock(name='index')
        sql = Mock(name='sql')
        table_fields = Mock(name='table_fields')
        table_name = Mock(name='table_name')
        task = ETLTask(sql, table_fields, table_name, index=index)
        task()
        create_index.assert_called_once_with(index, table_name)

    def test_doesnt_create_index_if_no_index_argument(
        self, execute_query, insert_data, drop_table, create_table, create_index
    ):
        sql = Mock(name='sql')
        table_fields = Mock(name='table_fields')
        table_name = Mock(name='table_name')
        task = ETLTask(sql, table_fields, table_name)
        task()
        create_index.assert_not_called()


class TestCreateIndex:
    @pytest.fixture(autouse=True)
    def setup(self, app_with_db):
        self.index = ['source', 'source_id']
        self.table_name = 'test_table'
        sql_create = '''
            create table {}
                (source varchar(50), source_id varchar(100), val int)
            '''
        sql_create = sql_create.format(self.table_name)
        execute_statement(sql_create)

    def get_indices(self):
        sql = '''
        select
            tablename,
            indexname,
            indexdef
        FROM pg_indexes
        WHERE schemaname = 'public' and tablename = %s'''
        return execute_query(sql, [self.table_name])

    def test(self):
        create_index(self.index, self.table_name)
        df = self.get_indices()
        assert len(df) == 1
        assert df['tablename'].values[0] == self.table_name
        assert df['indexname'].values[0] == f'{self.table_name}_index'

    def test_index_name_argument(self):
        index_name = 'index_name'
        create_index(self.index, self.table_name, index_name=index_name)
        df = self.get_indices()
        assert df['indexname'].values[0] == index_name
