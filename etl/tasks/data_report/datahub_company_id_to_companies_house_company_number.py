from etl.etl import ETLTask

sql = '''
select
  id as datahub_company_id,
  company_number as companies_house_company_number

from datahub_company

where company_number is not null and company_number != ''

order by 1

'''

table_fields = '''(
  datahub_company_id uuid primary key,
  companies_house_company_number varchar(12)
)'''

table_name = 'coi_datahub_company_id_to_companies_house_company_number'


class Task(ETLTask):
    def __init__(
            self,
            sql=sql,
            table_fields=table_fields,
            table_name=table_name,
            *args,
            **kwargs
    ):
        super().__init__(
            sql=sql,
            table_fields=table_fields,
            table_name=table_name,
            *args,
            **kwargs
        )
