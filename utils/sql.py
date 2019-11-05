import pandas as pd, psycopg2

def drop_table(connection, table_name, if_exists=True):
    cursor = connection.cursor()
    sql = '''drop table {} {}'''.format('if exists' if if_exists else '', table_name)
    cursor.execute(sql)
    connection.commit()

def execute_query(connection, sql, commit=True, values=[]):
    if connection.get_transaction_status() == psycopg2.extensions.TRANSACTION_STATUS_INERROR:
        connection.reset()
    cursor = connection.cursor()
    cursor.execute(sql, values)
    if commit is True:
        connection.commit()
    return cursor

def peek_at_table(connection, table, n_rows=10, schema='public'):
    if connection.get_transaction_status() == psycopg2.extensions.TRANSACTION_STATUS_INERROR:
        connection.reset()
    sql = '''
    select * from {schema}."{table}" limit {n_rows}
    '''.format(schema=schema, table=table, n_rows=n_rows)
    df = query_database(connection, sql)
    return df

def query_database(connection, sql, values=[]):
    cursor = execute_query(connection, sql, commit=False, values=values)
    rows = cursor.fetchall()
    columns = [d[0] for d in cursor.description]
    df = pd.DataFrame(rows, columns=columns)
    return df

def rename_table(connection, table_name_1, table_name_2):
    cursor = connection.cursor()
    sql = '''alter table {} rename to {}'''.format(table_name_1, table_name_2)
    cursor.execute(sql)
    connection.commit()

def table_exists(connection, table_name, schema='public'):
    cursor = connection.cursor()
    table_name = table_name
    sql = '''select * from information_schema.tables where table_schema=%s and table_name=%s'''
    cursor.execute(sql, [schema, table_name])
    rows = cursor.fetchall()
    return len(rows) > 0
    
