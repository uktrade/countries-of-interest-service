import pandas as pd, psycopg2

def execute_query(connection, sql, commit=True):
    if connection.get_transaction_status() == psycopg2.extensions.TRANSACTION_STATUS_INERROR:
        connection.reset()
    cursor = connection.cursor()
    cursor.execute(sql)
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

def query_database(connection, sql):
    cursor = execute_query(connection, sql, commit=False)
    rows = cursor.fetchall()
    columns = [d[0] for d in cursor.description]
    df = pd.DataFrame(rows, columns=columns)
    return df
