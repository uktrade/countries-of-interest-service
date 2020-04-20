from app.db.db_utils import execute_query
from app.db.models.internal import CountriesAndSectorsInterestTemp
from app.etl.tasks.datahub_company_export_country_history import Task


class TestDatahubCompanyExportCountryHistory:
    def test(self, add_country_territory_registry, add_datahub_company_export_country_history):

        add_datahub_company_export_country_history(
            [
                {
                    'company_id': '4737be58-3638-47da-b85e-5b9290f8025e',
                    'country': 'Japan',
                    'country_iso_alpha2_code': 'JP',
                    'history_date': '2020-01-01 02:00:00',
                    'history_id': '16475539-7189-41a3-88c4-506b8f5d2086',
                    'history_type': 'insert',
                    'status': 'future_interest',
                },
                {
                    'company_id': 'c2a48897-876c-4242-9504-1a73c048f57b',
                    'country': 'South Korea',
                    'country_iso_alpha2_code': 'KR',
                    'history_date': '2020-01-02 01:00:00',
                    'history_id': '2bf393fa-a94b-43ae-a907-5e21340414f8',
                    'history_type': 'delete',
                    'status': 'currently_exporting',
                },
            ]
        )

        add_country_territory_registry(
            [
                {'country_iso_alpha2_code': 'JP', 'name': 'Japan'},
                {'country_iso_alpha2_code': 'KR', 'name': 'South Korea'},
                {'country_iso_alpha2_code': 'UK', 'name': 'United Kingdom'},
            ]
        )

        task = Task()
        task()

        sql = f''' select * from {CountriesAndSectorsInterestTemp.__tablename__} '''
        df = execute_query(sql)

        assert len(df) == 2

        assert df['country'].values[0] == 'Japan'
        assert df['service'].values[0] == 'datahub'
        assert df['service_company_id'].values[0] == '4737be58-3638-47da-b85e-5b9290f8025e'
        assert df['source'].values[0] == 'company_export_country_history'
        assert df['source_id'].values[0] == '16475539-7189-41a3-88c4-506b8f5d2086'
        assert str(df['timestamp'].values[0]) == '2020-01-01T02:00:00.000000000'
        assert df['type'].values[0] == 'insert_future_interest'

        assert df['country'].values[1] == 'South Korea'
        assert df['service'].values[1] == 'datahub'
        assert df['service_company_id'].values[1] == 'c2a48897-876c-4242-9504-1a73c048f57b'
        assert df['source'].values[1] == 'company_export_country_history'
        assert df['source_id'].values[1] == '2bf393fa-a94b-43ae-a907-5e21340414f8'
        assert str(df['timestamp'].values[1]) == '2020-01-02T01:00:00.000000000'
        assert df['type'].values[1] == 'delete_currently_exporting'
