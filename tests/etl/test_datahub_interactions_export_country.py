import datetime

from flask import current_app

from app.db.models.internal import CountriesAndSectorsInterestTemp
from app.etl.tasks.datahub_interactions_export_country import Task


class TestTask:
    def test(self, add_country_territory_registry, add_datahub_interactions_export_country):

        add_datahub_interactions_export_country(
            [
                {
                    'country_iso_alpha2_code': 'TW',
                    'country_name': 'Taiwan',
                    'created_on': '2020-01-01 01:00:00',
                    'datahub_company_id': '3b5257b5-3a8b-436a-8781-385a30802437',
                    'datahub_interaction_export_country_id': '7aa47bd1-fa66-4dde-9b70-72da0091730e',
                    'datahub_interaction_id': 'eac5524f-9850-43d5-a064-66f6fc181c5c',
                    'status': 'future_interest',
                },
                {
                    'country_iso_alpha2_code': 'JM',
                    'country_name': 'Jamaica',
                    'created_on': '2020-01-02 02:00:00',
                    'datahub_company_id': 'cd628bc7-a2f5-4340-8134-2c248e859061',
                    'datahub_interaction_export_country_id': 'a7c79808-c870-4255-8e0b-17e635ea9244',
                    'datahub_interaction_id': '92f889e3-7b3c-4403-aebc-e80d39a855c8',
                    'status': 'currently_exporting',
                },
            ]
        )

        add_country_territory_registry(
            [
                {'country_iso_alpha2_code': 'TW', 'name': 'Taiwan'},
                {'country_iso_alpha2_code': 'JM', 'name': 'Jamaica'},
            ]
        )

        task = Task()
        task()

        session = current_app.db.session
        interactions_export_country = session.query(CountriesAndSectorsInterestTemp).all()

        assert len(interactions_export_country) == 2
        interaction_1, interaction_2 = interactions_export_country

        assert interaction_1.country == 'Taiwan'
        assert interaction_1.sector is None
        assert interaction_1.service == 'datahub'
        assert interaction_1.service_company_id == '3b5257b5-3a8b-436a-8781-385a30802437'
        assert interaction_1.source == 'interactions_export_country'
        assert interaction_1.source_id == '7aa47bd1-fa66-4dde-9b70-72da0091730e'
        assert interaction_1.timestamp == datetime.datetime(2020, 1, 1, 1)
        assert interaction_1.type == 'interested'

        assert interaction_2.country == 'Jamaica'
        assert interaction_2.sector is None
        assert interaction_2.service == 'datahub'
        assert interaction_2.service_company_id == 'cd628bc7-a2f5-4340-8134-2c248e859061'
        assert interaction_2.source == 'interactions_export_country'
        assert interaction_2.source_id == 'a7c79808-c870-4255-8e0b-17e635ea9244'
        assert interaction_2.timestamp == datetime.datetime(2020, 1, 2, 2)
        assert interaction_2.type == 'exported'
