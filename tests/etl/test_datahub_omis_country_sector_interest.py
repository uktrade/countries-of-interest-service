import numpy as np

from app.db.db_utils import execute_query
from app.db.models.internal import CountriesAndSectorsInterestTemp
from app.etl.tasks.datahub_omis_country_sector_interest import Task


class TestCountriesAndSectorsOfInterest:
    def test(
        self,
        add_country_territory_registry,
        add_datahub_omis,
        add_standardised_countries,
    ):
        add_datahub_omis(
            [
                {
                    'company_id': 'a4881825-6c7c-46f3-b638-6a1346274a6b',
                    'created_date': '2019-01-01 01:00',
                    'datahub_omis_order_id': '1ee5a16b-1a4b-4c84-838f-0d043579c9ba',
                    'market': 'CN',
                    'sector': 'Food',
                },
                {
                    'company_id': 'f89d85d2-78c7-484d-bf63-228f32bf8d26',
                    'created_date': '2019-02-02 02:00',
                    'datahub_omis_order_id': 'c0794724-c070-4c7e-a52c-89c0006bf7e6',
                    'market': 'UK',
                    'sector': 'Engineering',
                },
            ]
        )

        add_country_territory_registry(
            [{'country_iso_alpha2_code': 'UK', 'name': 'United Kingdom'}]
        )

        task = Task()
        task()

        sql = f'''select * from {CountriesAndSectorsInterestTemp.get_fq_table_name()}'''
        df = execute_query(sql)

        assert len(df) == 2

        # check first record
        assert (
            df['service_company_id'].values[0] == 'a4881825-6c7c-46f3-b638-6a1346274a6b'
        )
        assert df['company_match_id'].values[0] is None
        assert df['country'].values[0] is None
        assert df['sector'].values[0] == 'Food'
        assert df['source'].values[0] == 'datahub_omis'
        assert df['source_id'].values[0] == '1ee5a16b-1a4b-4c84-838f-0d043579c9ba'
        assert df['timestamp'].values[0] == np.datetime64('2019-01-01 01:00')

        # check second record
        assert (
            df['service_company_id'].values[1] == 'f89d85d2-78c7-484d-bf63-228f32bf8d26'
        )
        assert df['company_match_id'].values[1] is None
        assert df['country'].values[1] == 'United Kingdom'
        assert df['sector'].values[1] == 'Engineering'
        assert df['source'].values[1] == 'datahub_omis'
        assert df['source_id'].values[1] == 'c0794724-c070-4c7e-a52c-89c0006bf7e6'
        assert df['timestamp'].values[1] == np.datetime64('2019-02-02 02:00')
