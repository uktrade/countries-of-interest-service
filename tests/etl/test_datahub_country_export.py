import pandas as pd

from app.db.db_utils import execute_query
from app.db.models.internal import CountriesAndSectorsInterestTemp
from app.etl.tasks.datahub_country_export import Task


class TestExportCountries:
    def test(
        self,
        add_country_territory_registry,
        add_datahub_export_country_history,
        add_datahub_export_to_countries,
        add_standardised_countries,
    ):

        add_datahub_export_country_history(
            [
                {
                    'company_id': '16475539-7189-41a3-88c4-506b8f5d2086',
                    'country': 'Japan',
                    'country_iso_alpha2_code': 'JP',
                    'history_date': '2020-01-01 01:00:00',
                    'history_type': 'insert',
                    'id': '4737be58-3638-47da-b85e-5b9290f8025e',
                    'status': 'future_interest',
                },
                {
                    'company_id': '2bf393fa-a94b-43ae-a907-5e21340414f8',
                    'country': 'South Korea',
                    'country_iso_alpha2_code': 'KR',
                    'history_date': '2020-01-01 02:00:00',
                    'history_type': 'insert',
                    'id': 'c2a48897-876c-4242-9504-1a73c048f57b',
                    'status': 'currently_exporting',
                },
            ]
        )

        add_datahub_export_to_countries(
            [
                {
                    'company_id': '08c5f419-f85f-4051-b640-d3cfef8ef85d',
                    'country_iso_alpha2_code': 'UK',
                    'country': 'united kingdom',
                    'id': 0,
                },
                {
                    'company_id': '08c5f419-f85f-4051-b640-d3cfef8ef85d',
                    'country_iso_alpha2_code': 'CN',
                    'country': 'china',
                    'id': 1,
                },
            ]
        )

        add_country_territory_registry(
            [
                {'country_iso_alpha2_code': 'UK', 'name': 'United Kingdom'},
                {'country_iso_alpha2_code': 'KR', 'name': 'South Korea'},
            ]
        )

        add_standardised_countries(
            [{'id': 0, 'country': 'CN', 'standardised_country': 'China', 'similarity': 1}]
        )

        task = Task()
        task()

        sql = f''' select * from {CountriesAndSectorsInterestTemp.__tablename__} '''
        df = execute_query(sql)

        assert len(df) == 3

        assert df['service_company_id'].values[0] == '2bf393fa-a94b-43ae-a907-5e21340414f8'
        assert df['country'].values[0] == 'South Korea'
        assert df['service'].values[0] == 'datahub'
        assert df['source'].values[0] == 'export_country_history'
        assert df['source_id'].values[0] == 'c2a48897-876c-4242-9504-1a73c048f57b'
        assert str(df['timestamp'].values[0]) == '2020-01-01T02:00:00.000000000'
        assert df['type'].values[0] == 'insert_currently_exporting'

        assert df['service_company_id'].values[1] == '08c5f419-f85f-4051-b640-d3cfef8ef85d'
        assert df['country'].values[1] == 'United Kingdom'
        assert df['service'].values[1] == 'datahub'
        assert df['source'].values[1] == 'export_countries'
        assert df['source_id'].values[1] == '0'
        print(df['timestamp'].values[1])
        assert pd.isna(df['timestamp'].values[1])
        assert df['type'].values[1] == 'exported'

        assert df['service_company_id'].values[2] == '08c5f419-f85f-4051-b640-d3cfef8ef85d'
        assert df['service'].values[2] == 'datahub'
        assert df['country'].values[2] is None
        assert df['source'].values[2] == 'export_countries'
        assert df['source_id'].values[2] == '1'
        assert pd.isna(df['timestamp'].values[2])
        assert df['type'].values[2] == 'exported'
