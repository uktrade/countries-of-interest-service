from app.db.db_utils import (
    create_index,
    create_table,
    drop_table,
    execute_query,
    insert_data,
)


class ETLTask:
    def __init__(self, sql, table_fields, table_name, drop_table=False, index=None):
        self.drop_table = drop_table
        self.index = index
        self.sql = sql
        self.table_fields = table_fields
        self.table_name = table_name

    def __call__(self):

        if self.drop_table is True:
            drop_table(self.table_name)

        create_table(self.table_fields, self.table_name)

        df = execute_query(self.sql, raise_if_fail=True)

        insert_data(df, self.table_name)

        if self.index is not None:
            create_index(self.index, self.table_name)

        sql = ''' select count(1) from {} '''.format(self.table_name)
        df = execute_query(sql)

        return {
            'status': 'success',
            'rows': int(df.values[0][0]),
            'table': self.table_name,
        }
