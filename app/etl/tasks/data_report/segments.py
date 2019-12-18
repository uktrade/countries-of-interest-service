from app.etl import ETLTask

sql = '''
with level_2 as (
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

), results as (
    select id, segment from level_0 union
    select id, segment from level_01 union
    select id, segment from level_012
)

select distinct segment from results

'''

table_fields = '''(
  segment varchar(200) primary key
)'''

table_name = 'segments'


class Task(ETLTask):
    def __init__(
        self, sql=sql, table_fields=table_fields, table_name=table_name, *args, **kwargs
    ):
        super().__init__(
            sql=sql, table_fields=table_fields, table_name=table_name, *args, **kwargs
        )