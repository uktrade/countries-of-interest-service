import datetime

from etl.etl import ETLTask

now = datetime.datetime.now()

sql = '''
with matched_companies as (
    select
        id,
        company_number

    from company_company

    where company_number is not null
        and company_number not in ('', 'NotRegis', 'n/a', 'Not reg', 'N/A')

), n_companies as (
    select
        count(id)

    from company_company

), duplicates as (
    select
        company_number,
        count(1)

    from matched_companies

    group by 1

    having count(1) > 1

), n_duplicates as (
    select
        sum(count) as count

    from duplicates

), n_matches as (
    select
        count(1)

    from matched_companies

), results as (
    select
        n_companies.count as n_companies,
        n_matches.count as n_matches,
        n_duplicates.count as n_duplicates,
        n_matches.count - n_duplicates.count as n_unique_matches,
        '{now}' as timestamp

    from n_companies join n_matches on 1=1
        join n_duplicates on 1=1

)

select * from results

'''.format(
    now=now
)

table_fields = '''(
    n_companies int,
    n_matches int,
    n_duplicates int,
    n_unique_matches int,
    timestamp Timestamp
)'''

table_name = 'matched_companies'


class Task(ETLTask):
    def __init__(
        self, sql=sql, table_fields=table_fields, table_name=table_name, *args, **kwargs
    ):
        super().__init__(
            sql=sql, table_fields=table_fields, table_name=table_name, *args, **kwargs
        )
