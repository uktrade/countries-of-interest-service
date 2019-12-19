import pandas as pd

import sqlalchemy

from app.db.models import sql_alchemy


def execute_query(query, data=None, df=True, raise_if_fail=False):
    result_set = execute_statement(query, data, raise_if_fail)
    rows = result_set.fetchall()
    if df:
        return pd.DataFrame(rows, columns=result_set.keys())
    return rows


def execute_statement(stmt, data=None, raise_if_fail=False):
    connection = sql_alchemy.engine.connect()
    transaction = connection.begin()
    try:
        status = connection.execute(stmt, data)
        transaction.commit()
        connection.close()
        return status
    except sqlalchemy.exc.ProgrammingError as err:
        transaction.rollback()
        print('DB ERROR', err.orig)
        if raise_if_fail:
            raise err
        connection.close()


def table_exists(table_name, schema='public', materialized_view=False):
    _from = 'information_schema.tables' if not materialized_view else 'pg_matviews'
    _where = 'table_schema' if not materialized_view else 'schemaname'
    _and = 'table_name' if not materialized_view else 'matviewname'

    query = f"""
     SELECT EXISTS (
        SELECT 1
           FROM   {_from}
           WHERE  {_where} = '{schema}'
           AND    {_and} = '{table_name}'
     );
    """
    return list(execute_statement(query))[0][0]


def drop_table(fq_name):
    stmt = f'DROP TABLE IF EXISTS {fq_name} CASCADE'
    execute_statement(stmt)


def rename_table(table_name_1, table_name_2):
    sql = f'alter table {table_name_1} rename to {table_name_2}'
    execute_statement(sql)


def create_table(fields, table_name):
    sql = f'create table if not exists {table_name} {fields}'
    execute_statement(sql)


def create_index(index, table_name, index_name=None):
    index_name = index_name if index_name else '{}_index'.format(table_name)
    index_str = '(' + ','.join(index) + ')'
    sql = f'create index if not exists {index_name} on {table_name} {index_str}'
    execute_statement(sql, data=index)


def insert_data(df, table_name):
    if len(df):
        sql = f'insert into {table_name} values'
        for i, values in enumerate(df.values):
            # quote string values
            values = [
                "'{}'".format(val) if type(val) in [str, pd.Timestamp] else val
                for val in values
            ]
            values = ['Null' if pd.isnull(val) else val for val in values]
            values = ['{}'.format(val) for val in values]
            sql += f"\n\t({', '.join(values)})"
            sql += ', ' if i != len(df) - 1 else ''
        sql += '\n\ton conflict do nothing'
        execute_statement(sql)
