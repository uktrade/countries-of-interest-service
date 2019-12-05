import datetime
from etl.etl import ETLTask

now = datetime.datetime.now()

sql = '''
with n_companies as (
    select
        count(id) as n_companies
        
    from company_company
    
), n_sectors as (
    select
        count(distinct sector_id) as n_sectors
        
    from company_company
    
), n_matched_companies as (
    select
        count(sector_id) as n_matched_companies
        
    from company_company
        
), results as (
    select
        n_companies,
        n_sectors,
        n_matched_companies,
        '{now}' as timestamp
        
    from n_companies join n_sectors on 1=1
        join n_matched_companies on 1=1
        
)

select * from results

'''.format(
    now=now
)

table_fields = '''(
    n_companies int, 
    n_sectors int,
    n_matches int,
    timestamp Timestamp
)'''

table_name = 'sector_matches'


class Task(ETLTask):
    def __init__(
        self, sql=sql, table_fields=table_fields, table_name=table_name, *args, **kwargs
    ):
        super().__init__(
            sql=sql, table_fields=table_fields, table_name=table_name, *args, **kwargs
        )
