from app.db.db_utils import execute_query
from app.db.models.internal import CountriesAndSectorsInterestTemp
from app.etl.tasks.datahub_country_export import Task


class TestExportCountries:
    def test(
        self,
        add_country_territory_registry,
        add_datahub_export_to_countries,
        add_standardised_countries,
    ):

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
            [{'country_iso_alpha2_code': 'UK', 'name': 'United Kingdom'}]
        )

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

        sql = f''' select * from {CountriesAndSectorsInterestTemp.__tablename__} '''
        df = execute_query(sql)

        assert len(df) == 2

        assert (
            df['service_company_id'].values[0] == '08c5f419-f85f-4051-b640-d3cfef8ef85d'
        )
        assert df['country'].values[0] == 'United Kingdom'
        assert df['source'].values[0] == 'datahub_export_countries'
        assert df['source_id'].values[0] == '0'
        assert not df['timestamp'].values[0]

        assert (
            df['service_company_id'].values[1] == '08c5f419-f85f-4051-b640-d3cfef8ef85d'
        )
        assert df['country'].values[1] is None
        assert df['source'].values[1] == 'datahub_export_countries'
        assert df['source_id'].values[1] == '1'
        assert not df['timestamp'].values[1]
