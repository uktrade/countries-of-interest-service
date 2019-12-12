import pandas as pd

from utils.sql import execute_query, query_database


def create_index(connection, index, table_name, index_name=None):
    index_name = index_name if index_name else '{}_index'.format(table_name)
    sql = '''create index if not exists {} on {} {}'''
    index_str = '(' + ','.join(index) + ')'
    sql = sql.format(index_name, table_name, index_str)
    execute_query(connection, sql, values=index)


def create_table(connection, fields, table_name):
    sql = ''' create table if not exists {} {} '''.format(table_name, fields)
    execute_query(connection, sql)


def drop_table(connection, table_name):
    sql = ''' drop table if exists {table_name} '''.format(table_name=table_name)
    execute_query(connection, sql)


def insert_data(df, output_connection, table_name):
    sql = '''insert into {} values'''.format(table_name)
    for i, values in enumerate(df.values):
        # quote string values
        values = [
            "'{}'".format(val) if type(val) in [str, pd.Timestamp] else val
            for val in values
        ]
        values = ['Null' if pd.isnull(val) else val for val in values]
        values = ['{}'.format(val) for val in values]
        sql += '\n\t({})'.format(', '.join(values))
        sql += ', ' if i != len(df) - 1 else ''
    sql += '\n\ton conflict do nothing'
    execute_query(output_connection, sql)


class ETLTask:
    def __init__(
        self, connection, sql, table_fields, table_name, drop_table=False, index=None,
    ):
        self.connection = connection
        self.drop_table = drop_table
        self.index = index
        self.sql = sql
        self.table_fields = table_fields
        self.table_name = table_name

    def __call__(self):
        if self.drop_table is True:
            print('\033[31mdropping table: {}\033[0m'.format(self.table_name))
            drop_table(self.connection, self.table_name)

        # print('\033[31mcreate table: {}\033[0m'.format(self.table_name))
        create_table(self.connection, self.table_fields, self.table_name)

        # print('\033[31mextract data\033[0m')
        df = query_database(self.connection, self.sql)
        # print(df.head())

        # print('\033[31mingest data\033[0m')
        insert_data(df, self.connection, self.table_name)

        if self.index is not None:
            create_index(self.connection, self.index, self.table_name)

        # print('\033[31mcheck data\033[0m')
        # sql = ''' select * from {} limit 5 '''.format(self.table_name)
        # df = query_database(self.connection, sql)
        # print(df.head())

        # print('\033[31mcheck data size\033[0m')
        sql = ''' select count(1) from {} '''.format(self.table_name)
        df = query_database(self.connection, sql)
        # print('{} rows'.format(df.values[0][0]))

        return {
            'status': 'success',
            'rows': int(df.values[0][0]),
            'table': self.table_name,
        }
