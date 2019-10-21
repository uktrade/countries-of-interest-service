from etl.etl import ETLTask

sql = '''
with omis_sectors_of_interest as (
    select distinct
      company_id,
      sector_id,
      'datahub_order' as source,
      id::varchar(100) as source_id,
      created_on as timestamp

    from order_order

    where sector_id is not null

), level_2 as (
  select id, parent_id, segment from metadata_sector where level = 2
), level_1 as (
  select id, parent_id, segment from metadata_sector where level = 1
), level_0 as (
  select id, segment from metadata_sector where level = 0
), level_012 as (
  select
    l2.id,
    concat(l0.segment, ':', l1.segment, ':', l2.segment) as segment

  from level_2 l2 join level_1 l1 on l2.parent_id = l1.id
    join level_0 l0 on l1.parent_id = l0.id

), level_01 as (
  select
    l1.id,
    concat(l1.segment, ':', l0.segment) as segment

  from level_1 l1 join level_0 l0 on l1.parent_id = l0.id

), segments as (
  select id, segment from level_0 union
  select id, segment from level_01 union
  select id, segment from level_012

),  results as (
  select
    company_number as companies_house_company_number,
    segment as sector_as_interest,
    source,
    source_id,
    timestamp

  from company_company join omis_sectors_of_interest on 
      company_company.id = omis_sectors_of_interest.company_id    
    join segments on segments.id = omis_sectors_of_interest.sector_id
  
  where company_number != ''
    and company_number is not null

)

select * from results order by 1, 5

'''

table_fields = '''(
    companies_house_company_number varchar(12), 
    sector_of_interest varchar(200), 
    source varchar(50), 
    source_id varchar(100),
    timestamp timestamp,
    primary key (companies_house_company_number, sector_of_interest, source, source_id)
)'''

table_name = 'sectors_of_interest_by_companies_house_company_number'

class Task(ETLTask):

    def __init__(self, sql=sql, table_fields=table_fields, table_name=table_name, *args, **kwargs):
        super().__init__(
            sql=sql,
            table_fields=table_fields,
            table_name=table_name,
            *args,
            **kwargs
        )
