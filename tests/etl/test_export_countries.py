from app.db.db_utils import execute_query, execute_statement
from app.etl.tasks.core.export_countries import Task


class TestExportCountries:
    def test(self, app_with_db):
        sql = (
            'create table datahub_export_countries '
            '(company_id uuid, country_iso_alpha2_code varchar(2), id int)'
        )
        execute_statement(sql)
        values = [
            ('a4881825-6c7c-46f3-b638-6a1346274a6b', 'CN', '0'),
            ('f89d85d2-78c7-484d-bf63-228f32bf8d26', 'UK', '1'),
        ]
        sql = 'insert into datahub_export_countries ' 'values (%s, %s, %s)'
        execute_statement(sql, values)

        task = Task()
        task()

        sql = ''' select * from coi_export_countries '''
        df = execute_query(sql)

        assert len(df) == 2
        assert df['company_id'].values[0] == 'a4881825-6c7c-46f3-b638-6a1346274a6b'

        assert df['export_country'].values[0] == 'CN'
        assert df['source'].values[0] == 'datahub_export_countries'
        assert df['source_id'].values[0] == '0'
        assert not df['timestamp'].values[0]
        assert df['company_id'].values[1] == 'f89d85d2-78c7-484d-bf63-228f32bf8d26'

        assert df['export_country'].values[1] == 'UK'
        assert df['source'].values[1] == 'datahub_export_countries'
        assert df['source_id'].values[1] == '1'
        assert not df['timestamp'].values[1]
