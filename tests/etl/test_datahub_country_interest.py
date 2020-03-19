import pandas as pd

from app.db.db_utils import execute_query
from app.db.models.internal import CountriesAndSectorsInterestTemp
from app.etl.tasks.datahub_country_interest import Task


class TestCountriesAndSectorsOfInterest:
    def test(
        self,
        add_country_territory_registry,
        add_datahub_export_country_history,
        add_datahub_future_interest_countries,
        add_standardised_countries,
    ):
        add_datahub_export_country_history(
            [
                {
                    'company_id': '16475539-7189-41a3-88c4-506b8f5d2086',
                    'country': 'Japan',
                    'country_iso_alpha2_code': 'JP',
                    'history_date': '2020-01-01 01:00:00',
                    'history_id': '4737be58-3638-47da-b85e-5b9290f8025e',
                    'history_type': 'insert',
                    'status': 'future_interest',
                },
                {
                    'company_id': '2bf393fa-a94b-43ae-a907-5e21340414f8',
                    'country': 'South Korea',
                    'country_iso_alpha2_code': 'KR',
                    'history_date': '2020-01-01 02:00:00',
                    'history_id': 'c2a48897-876c-4242-9504-1a73c048f57b',
                    'history_type': 'insert',
                    'status': 'currently_exporting',
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

        add_country_territory_registry(
            [
                {'country_iso_alpha2_code': 'UK', 'name': 'United Kingdom'},
                {'country_iso_alpha2_code': 'JP', 'name': 'Japan'},
            ]
        )

        add_standardised_countries(
            [{'id': 0, 'country': 'CN', 'standardised_country': 'China', 'similarity': 1}]
        )

        task = Task()
        task()

        sql = f'''select * from {CountriesAndSectorsInterestTemp.__tablename__}'''
        df = execute_query(sql)

        assert len(df) == 2

        # check record
        assert df['service_company_id'].values[0] == '16475539-7189-41a3-88c4-506b8f5d2086'
        assert df['company_match_id'].values[0] is None
        assert df['country'].values[0] == 'Japan'
        assert df['service'].values[0] == 'datahub'
        assert df['source'].values[0] == 'export_country_history'
        assert df['source_id'].values[0] == '4737be58-3638-47da-b85e-5b9290f8025e'
        assert df['type'].values[0] == 'insert_future_interest'
        assert str(df['timestamp'].values[0]) == '2020-01-01T01:00:00.000000000'

        assert df['service_company_id'].values[1] == '08c5f419-f85f-4051-b640-d3cfef8ef85d'
        assert df['company_match_id'].values[1] is None
        assert df['country'].values[1] == 'United Kingdom'
        assert df['service'].values[1] == 'datahub'
        assert df['source'].values[1] == 'future_interest_countries'
        assert df['source_id'].values[1] == '0'
        assert df['type'].values[1] == 'interested'
        assert pd.isna(df['timestamp'].values[1])
