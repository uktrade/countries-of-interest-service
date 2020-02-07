import datetime
from unittest.mock import patch

from app.config.constants import Source, Task
from app.db.db_utils import execute_query, execute_statement
from app.etl.tasks import populate_database


@patch('app.etl.tasks.source_data_extraction.' 'ExtractExportWins.__call__')
@patch(
    'app.etl.tasks.source_data_extraction.'
    'ExtractCountriesAndTerritoriesReferenceDataset.__call__'
)
@patch('app.etl.tasks.source_data_extraction.' 'ExtractDatahubCompanyDataset.__call__')
@patch('app.etl.tasks.source_data_extraction.' 'ExtractDatahubContactDataset.__call__')
@patch('app.etl.tasks.source_data_extraction.' 'ExtractDatahubExportToCountries.__call__')
@patch('app.etl.tasks.source_data_extraction.' 'ExtractDatahubFutureInterestCountries.__call__')
@patch('app.etl.tasks.source_data_extraction.' 'ExtractDatahubInteractions.__call__')
@patch('app.etl.tasks.source_data_extraction.ExtractDatahubOmis.__call__')
@patch('app.etl.tasks.country_standardisation.PopulateStandardisedCountriesTask.__call__')
@patch('app.etl.tasks.export_wins_country.Task.__call__')
@patch('app.etl.tasks.company_matching.Task.__call__')
@patch('app.etl.tasks.datahub_country_interest.Task.__call__')
@patch('app.etl.tasks.datahub_country_export.Task.__call__')
@patch('app.etl.tasks.datahub_omis_country_sector_interest.Task.__call__')
@patch('app.etl.tasks.interactions_analysed.Task.__call__')
@patch('app.etl.tasks.datahub_interaction_country.Task.__call__')
class TestPopulateDatabase:
    def _patch_tasks(
        self,
        populate_datahub_interaction_country_task,
        populate_interactions_analysed_task,
        populate_datahub_omis_country_sector_interest_task,
        populate_datahub_country_export_task,
        populate_datahub_country_interest_task,
        populate_export_wins_task,
        populate_company_matching_task,
        populate_standardised_countries_task,
        extract_datahub_omis,
        extract_datahub_interactions,
        extract_datahub_future_interest_countries,
        extract_datahub_export_to_countries,
        extract_datahub_contact_dataset,
        extract_datahub_company_dataset,
        extract_countries_and_territories_reference_dataset,
        extract_export_wins,
    ):
        extract_countries_and_territories_reference_dataset.return_value = {
            'dataset': 'countries_and_territories_reference_dataset'
        }
        extract_datahub_contact_dataset.return_value = {'dataset': 'datahub_contact_dataset'}
        extract_datahub_company_dataset.return_value = {'dataset': 'datahub_company_dataset'}
        extract_datahub_export_to_countries.return_value = {
            'dataset': 'datahub_export_to_countries'
        }
        extract_datahub_future_interest_countries.return_value = {
            'dataset': 'datahub_future_interest_countries'
        }
        extract_datahub_interactions.return_value = {'dataset': 'datahub_interaction'}
        extract_datahub_omis.return_value = {'dataset': 'datahub_omis'}
        extract_export_wins.return_value = {'dataset': 'export_wins'}

        populate_standardised_countries_task.return_value = {
            'dataset': 'populate_standardised_countries'
        }
        populate_company_matching_task.return_value = {'dataset': 'populate_company_matching'}
        populate_datahub_country_interest_task.return_value = {
            'dataset': 'populate_datahub_country_interest'
        }
        populate_datahub_country_export_task.return_value = {
            'dataset': 'populate_datahub_country_export'
        }
        populate_datahub_omis_country_sector_interest_task.return_value = {
            'dataset': 'populate_datahub_omis_country_sector_interest'
        }

        populate_datahub_country_interest_task.return_value = {
            'dataset': 'populate_datahub_country_interest'
        }
        populate_datahub_interaction_country_task.return_value = {
            'dataset': 'populate_datahub_interaction_country'
        }
        populate_interactions_analysed_task.return_value = {
            'dataset': 'populate_interactions_analysed'
        }
        populate_export_wins_task.return_value = {'dataset': 'populate_export_wins'}

    @patch('app.etl.tasks.execute_statement')
    def test_tasks_are_run(
        self,
        execute_statement,
        populate_datahub_interaction_country_task,
        populate_interactions_analysed_task,
        populate_datahub_omis_country_sector_interest_task,
        populate_datahub_country_export_task,
        populate_datahub_country_interest_task,
        populate_export_wins_task,
        populate_company_matching_task,
        populate_standardised_countries_task,
        extract_datahub_omis,
        extract_datahub_interactions,
        extract_datahub_future_interest_countries,
        extract_datahub_export_to_countries,
        extract_datahub_contact_dataset,
        extract_datahub_company_dataset,
        extract_countries_and_territories_reference_dataset,
        extract_export_wins,
        app_with_db,
    ):
        execute_statement.return_value = None
        self._patch_tasks(
            populate_datahub_interaction_country_task,
            populate_interactions_analysed_task,
            populate_datahub_omis_country_sector_interest_task,
            populate_datahub_country_export_task,
            populate_datahub_country_interest_task,
            populate_export_wins_task,
            populate_company_matching_task,
            populate_standardised_countries_task,
            extract_datahub_omis,
            extract_datahub_interactions,
            extract_datahub_future_interest_countries,
            extract_datahub_export_to_countries,
            extract_datahub_contact_dataset,
            extract_datahub_company_dataset,
            extract_countries_and_territories_reference_dataset,
            extract_export_wins,
        )

        output = populate_database(True, [], [])

        assert extract_countries_and_territories_reference_dataset.called is True
        assert extract_datahub_company_dataset.called is True
        assert extract_datahub_contact_dataset.called is True
        assert extract_datahub_export_to_countries.called is True
        assert extract_datahub_future_interest_countries.called is True
        assert extract_datahub_interactions.called is True
        assert extract_datahub_omis.called is True
        assert extract_export_wins.called is True

        assert populate_standardised_countries_task.called is True
        assert populate_datahub_country_interest_task.called is True
        assert populate_datahub_country_export_task.called is True
        assert populate_datahub_omis_country_sector_interest_task.called is True
        assert populate_datahub_interaction_country_task.called is True
        assert populate_export_wins_task.called is True
        assert populate_interactions_analysed_task.called is True
        assert populate_company_matching_task.called is True

        assert output == {
            'output': [
                {'dataset': 'countries_and_territories_reference_dataset'},
                {'dataset': 'datahub_company_dataset'},
                {'dataset': 'datahub_contact_dataset'},
                {'dataset': 'datahub_export_to_countries'},
                {'dataset': 'datahub_interaction'},
                {'dataset': 'datahub_future_interest_countries'},
                {'dataset': 'datahub_omis'},
                {'dataset': 'export_wins'},
                {'dataset': 'populate_standardised_countries'},
                {'dataset': 'populate_interactions_analysed'},
                {'dataset': 'populate_datahub_country_export'},
                {'dataset': 'populate_datahub_country_interest'},
                {'dataset': 'populate_datahub_omis_country_sector_interest'},
                {'dataset': 'populate_company_matching'},
                {'dataset': 'populate_datahub_interaction_country'},
                {'dataset': 'populate_export_wins'},
            ]
        }

    @patch('app.etl.tasks.execute_statement')
    def test_only_specified_tasks_are_run(
        self,
        execute_statement,
        populate_datahub_interaction_country_task,
        populate_interactions_analysed_task,
        populate_datahub_omis_country_sector_interest_task,
        populate_datahub_country_export_task,
        populate_datahub_country_interest_task,
        populate_export_wins_task,
        populate_company_matching_task,
        populate_standardised_countries_task,
        extract_datahub_omis,
        extract_datahub_interactions,
        extract_datahub_future_interest_countries,
        extract_datahub_export_to_countries,
        extract_datahub_contact_dataset,
        extract_datahub_company_dataset,
        extract_countries_and_territories_reference_dataset,
        extract_export_wins,
        app_with_db,
    ):
        execute_statement.return_value = None
        self._patch_tasks(
            populate_datahub_interaction_country_task,
            populate_interactions_analysed_task,
            populate_datahub_omis_country_sector_interest_task,
            populate_datahub_country_export_task,
            populate_datahub_country_interest_task,
            populate_export_wins_task,
            populate_company_matching_task,
            populate_standardised_countries_task,
            extract_datahub_omis,
            extract_datahub_interactions,
            extract_datahub_future_interest_countries,
            extract_datahub_export_to_countries,
            extract_datahub_contact_dataset,
            extract_datahub_company_dataset,
            extract_countries_and_territories_reference_dataset,
            extract_export_wins,
        )

        output = populate_database(
            True, [Source.DATAHUB_COMPANY.value], [Task.COUNTRY_SECTOR_INTEREST.value]
        )

        assert extract_countries_and_territories_reference_dataset.called is False
        assert extract_datahub_contact_dataset.called is False
        assert extract_datahub_company_dataset.called is True
        assert extract_datahub_export_to_countries.called is False
        assert extract_datahub_future_interest_countries.called is False
        assert extract_datahub_interactions.called is False
        assert extract_datahub_omis.called is False
        assert extract_export_wins.called is False

        assert populate_standardised_countries_task.called is False
        assert populate_datahub_country_interest_task.called is True
        assert populate_datahub_country_export_task.called is True
        assert populate_datahub_omis_country_sector_interest_task.called is True
        assert populate_datahub_interaction_country_task.called is True
        assert populate_export_wins_task.called is True
        assert populate_interactions_analysed_task.called is False
        assert populate_company_matching_task.called is True

        assert output == {
            'output': [
                {'dataset': 'datahub_company_dataset'},
                {'dataset': 'populate_datahub_country_export'},
                {'dataset': 'populate_datahub_country_interest'},
                {'dataset': 'populate_datahub_omis_country_sector_interest'},
                {'dataset': 'populate_company_matching'},
                {'dataset': 'populate_datahub_interaction_country'},
                {'dataset': 'populate_export_wins'},
            ]
        }

    @patch('app.etl.tasks.datetime')
    def test_updates_task_status_to_success(
        self,
        mock_datetime,
        populate_datahub_interaction_country_task,
        populate_interactions_analysed_task,
        populate_datahub_omis_country_sector_interest_task,
        populate_datahub_country_export_task,
        populate_datahub_country_interest_task,
        populate_export_wins_task,
        populate_company_matching_task,
        populate_standardised_countries_task,
        extract_datahub_omis,
        extract_datahub_interactions,
        extract_datahub_future_interest_countries,
        extract_datahub_export_to_countries,
        extract_datahub_contact_dataset,
        extract_datahub_company_dataset,
        extract_countries_and_territories_reference_dataset,
        extract_export_wins,
        app_with_db,
    ):
        self._patch_tasks(
            populate_datahub_interaction_country_task,
            populate_interactions_analysed_task,
            populate_datahub_omis_country_sector_interest_task,
            populate_datahub_country_export_task,
            populate_datahub_country_interest_task,
            populate_export_wins_task,
            populate_company_matching_task,
            populate_standardised_countries_task,
            extract_datahub_omis,
            extract_datahub_interactions,
            extract_datahub_future_interest_countries,
            extract_datahub_export_to_countries,
            extract_datahub_contact_dataset,
            extract_datahub_company_dataset,
            extract_countries_and_territories_reference_dataset,
            extract_export_wins,
        )
        mock_datetime.datetime.now.return_value = datetime.datetime(2019, 1, 1, 2)
        sql = "insert into etl_status (status, timestamp) values" "('RUNNING', '2019-01-01 01:00')"
        execute_statement(sql)
        populate_database(True, [], [])
        sql = 'select * from etl_status'
        rows = execute_query(sql, df=False)
        assert len(rows) == 1
        assert rows[0][1] == 'SUCCESS'
        assert rows[0][2] == datetime.datetime(2019, 1, 1, 2)
