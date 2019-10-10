import datetime
from etl.etl import ETLTask

now = datetime.datetime.now()

sql = '''
with n_companies as (
  select
    count(id) as n_companies

  from company_company

), n_companies_with_countries_of_interest as (
  select
    count(distinct id) as n_companies_with_countries_of_interest
    
  from company_company_future_interest_countries

)

select
  n_companies,
  n_companies_with_countries_of_interest,
  '{now}' as timestamp

from n_companies join n_companies_with_countries_of_interest on 1=1

'''.format(now=now)

table_fields = '''(
    n_companies int,
    n_companies_with_countries_of_interest int,
    timestamp Timestamp
)'''

table_name = 'companies_with_countries_of_interest'

class Task(ETLTask):

    def __init__(self, sql=sql, table_fields=table_fields, table_name=table_name, *args, **kwargs):
        super().__init__(
            sql=sql,
            table_fields=table_fields,
            table_name=table_name,
            *args,
            **kwargs
        )
