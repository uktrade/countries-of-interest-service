from app.db.db_utils import execute_query
from app.db.models.internal import CountriesAndSectorsInterestTemp
from app.etl.tasks.export_wins_country import Task


class TestExportCountries:
    def test(
        self, add_country_territory_registry, add_standardised_countries, add_export_wins,
    ):

        add_export_wins(
            [
                {
                    'export_wins_id': '08c5f419-f85f-4051-b640-d3cfef8ef85d',
                    'sector': 'Aerospace',
                    'company_name': 'Aero space',
                    'export_wins_company_id': '2',
                    'country': 'United Kingdom',
                },
                {
                    'export_wins_id': '08c5f419-f85f-4051-b640-d3cfef8e1213',
                    'sector': 'Food',
                    'company_name': 'Aero',
                    'export_wins_company_id': '3',
                    'country': 'China',
                },
            ]
        )

        add_country_territory_registry(
            [{'country_iso_alpha2_code': 'UK', 'name': 'United Kingdom'}]
        )

        add_standardised_countries(
            [
                {'id': 0, 'country': 'China', 'standardised_country': 'China', 'similarity': 1},
                {
                    'id': 1,
                    'country': 'United Kingdom',
                    'standardised_country': 'United Kingdom',
                    'similarity': 91,
                },
            ]
        )

        task = Task()
        task()

        sql = f''' select * from {CountriesAndSectorsInterestTemp.__tablename__} '''
        df = execute_query(sql)

        assert len(df) == 2

        assert df['service_company_id'].values[1] == '2'
        assert df['country'].values[1] == 'United Kingdom'
        assert df['source'].values[1] == 'export_wins'
        assert df['type'].values[1] == 'exported'
        assert df['source_id'].values[1] == '08c5f419-f85f-4051-b640-d3cfef8ef85d'
        assert not df['timestamp'].values[1]

        assert df['service_company_id'].values[0] == '3'
        assert not df['country'].values[0]
        assert df['source'].values[0] == 'export_wins'
        assert df['type'].values[0] == 'exported'
        assert df['source_id'].values[0] == '08c5f419-f85f-4051-b640-d3cfef8e1213'
        assert not df['timestamp'].values[0]
