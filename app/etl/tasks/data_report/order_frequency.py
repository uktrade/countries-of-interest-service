import datetime

from app.etl import ETLTask

now = datetime.datetime.now()

sql = '''
select
    date_trunc('day', created_on) as date,
    count(created_on) as count,
    '{now}' as timestamp

from order_order

group by 1

order by 1

'''.format(
    now=now
)

table_fields = '''(
    date Timestamp,
    count int,
    timestamp Timestamp
)'''

table_name = 'order_frequency_by_date'


class Task(ETLTask):
    def __init__(
        self, sql=sql, table_fields=table_fields, table_name=table_name, *args, **kwargs
    ):
        super().__init__(
            sql=sql, table_fields=table_fields, table_name=table_name, *args, **kwargs
        )
