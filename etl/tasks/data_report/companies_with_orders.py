import datetime

from etl.etl import ETLTask

now = datetime.datetime.now()

sql = '''
with n_companies as (
  select
    count(id) as n_companies

  from company_company

), n_companies_with_orders as (
  select
    count(distinct id) as n_companies_with_orders

  from order_order

)

select
  n_companies,
  n_companies_with_orders,
  '{now}' as timestamp

from n_companies join n_companies_with_orders on 1=1

'''.format(
    now=now
)

table_fields = '''(
    n_companies int,
    n_companies_with_orders int,
    timestamp Timestamp
)'''

table_name = 'companies_with_orders'


class Task(ETLTask):
    def __init__(
        self, sql=sql, table_fields=table_fields, table_name=table_name, *args, **kwargs
    ):
        super().__init__(
            sql=sql, table_fields=table_fields, table_name=table_name, *args, **kwargs
        )
