from app.db.db_utils import (
    execute_query,
    insert_data,
)


class ETLTask:
    def __init__(self, sql, model, drop_table=False):
        self.drop_table = drop_table
        self.sql = sql
        self.model = model
        self.table_name = model.__tablename__

    def __call__(self):

        if self.drop_table is True:
            self.model.drop_table()

        self.model.create_table()

        df = execute_query(self.sql, raise_if_fail=True)
        table_name = self.model.get_fq_table_name()
        insert_data(df, table_name)

        sql = f''' select count(1) from {table_name} '''
        df = execute_query(sql)

        return {
            'status': 'success',
            'rows': int(df.values[0][0]),
            'table': self.table_name,
        }
