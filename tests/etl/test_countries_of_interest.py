import numpy as np

from app.db.db_utils import execute_query
from app.etl.tasks.countries_of_interest import Task


class TestCountriesAndSectorsOfInterest:
    def test(
        self,
        add_country_territory_registry,
        add_datahub_future_interest_countries,
        add_datahub_omis,
        add_standardised_countries,
    ):
        add_datahub_omis(
            [
                {
                    'company_id': 'a4881825-6c7c-46f3-b638-6a1346274a6b',
                    'created_date': '2019-01-01 01:00',
                    'id': '1ee5a16b-1a4b-4c84-838f-0d043579c9ba',
                    'market': 'CN',
                    'sector': 'Food',
                },
                {
                    'company_id': 'f89d85d2-78c7-484d-bf63-228f32bf8d26',
                    'created_date': '2019-02-02 02:00',
                    'id': 'c0794724-c070-4c7e-a52c-89c0006bf7e6',
                    'market': 'UK',
                    'sector': 'Engineering',
                },
            ]
        )

        add_datahub_future_interest_countries(
            [
                {
                    'company_id': '08c5f419-f85f-4051-b640-d3cfef8ef85d',
                    'country_iso_alpha2_code': 'UK',
                    'id': 0,
                }
            ]
        )

        add_country_territory_registry([{'id': 'UK', 'name': 'United Kingdom'}])

        add_standardised_countries(
            [
                {
                    'id': 0,
                    'country': 'CN',
                    'standardised_country': 'China',
                    'similarity': 1,
                }
            ]
        )

        task = Task()
        task()

        sql = '''select * from coi_countries_of_interest'''
        df = execute_query(sql)

        assert len(df) == 3

        assert df['company_id'].values[0] == '08c5f419-f85f-4051-b640-d3cfef8ef85d'
        assert df['country_of_interest'].values[0] == 'UK'
        assert df['standardised_country'].values[0] == 'United Kingdom'
        assert df['source'].values[0] == 'datahub_future_interest_countries'
        assert df['source_id'].values[0] == '0'
        assert np.isnat(df['timestamp'].values[0])

        assert df['company_id'].values[1] == 'a4881825-6c7c-46f3-b638-6a1346274a6b'
        assert df['country_of_interest'].values[1] == 'CN'
        assert df['standardised_country'].values[1] is None
        assert df['source'].values[1] == 'omis'
        assert df['source_id'].values[1] == '1ee5a16b-1a4b-4c84-838f-0d043579c9ba'
        assert df['timestamp'].values[1] == np.datetime64('2019-01-01 01:00')

        assert df['company_id'].values[2] == 'f89d85d2-78c7-484d-bf63-228f32bf8d26'
        assert df['country_of_interest'].values[2] == 'UK'
        assert df['standardised_country'].values[2] == 'United Kingdom'
        assert df['source'].values[2] == 'omis'
        assert df['source_id'].values[2] == 'c0794724-c070-4c7e-a52c-89c0006bf7e6'
        assert df['timestamp'].values[2] == np.datetime64('2019-02-02 02:00')

        sql = '''
        select
            tablename,
            indexname,
            indexdef
        FROM pg_indexes
        WHERE schemaname = 'public'
          and tablename = 'coi_countries_of_interest'
        '''
        df = execute_query(sql)
        assert len(df) >= 1  # index created
