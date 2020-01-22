import datetime
from unittest.mock import patch

from app.db.db_utils import execute_query, execute_statement
from app.etl.tasks import populate_database


@patch(
    'app.etl.tasks.source_data_extraction.'
    'ExtractCountriesAndTerritoriesReferenceDataset.__call__'
)
@patch('app.etl.tasks.source_data_extraction.' 'ExtractDatahubCompanyDataset.__call__')
@patch(
    'app.etl.tasks.source_data_extraction.' 'ExtractDatahubExportToCountries.__call__'
)
@patch(
    'app.etl.tasks.source_data_extraction.'
    'ExtractDatahubFutureInterestCountries.__call__'
)
@patch('app.etl.tasks.source_data_extraction.' 'ExtractDatahubInteractions.__call__')
@patch('app.etl.tasks.source_data_extraction.ExtractDatahubOmis.__call__')
@patch(
    'app.etl.tasks.country_standardisation.PopulateStandardisedCountriesTask.__call__'
)
@patch('app.etl.tasks.interactions_analysed.Task.__call__')
@patch('app.etl.tasks.export_countries.Task.__call__')
@patch('app.etl.tasks.countries_and_sectors_of_interest.Task.__call__')
@patch('app.etl.tasks.countries_of_interest.Task.__call__')
@patch('app.etl.tasks.sectors_of_interest.Task.__call__')
@patch('app.etl.tasks.mentioned_in_interactions.Task.__call__')
class TestPopulateDatabase:
    @patch('app.etl.tasks.execute_statement')
    def test_tasks_are_run(
        self,
        execute_statement,
        populate_mentioned_in_interactions_task,
        populate_sectors_of_interest_task,
        populate_countries_of_interest_task,
        populate_countries_and_sectors_of_interest_task,
        populate_export_to_countries_task,
        populate_interactions_analysed_task,
        populate_standardised_countries_task,
        extract_datahub_omis,
        extract_datahub_interactions,
        extract_datahub_future_interest_countries,
        extract_datahub_export_to_countries,
        extract_datahub_company_dataset,
        extract_countries_and_territories_reference_dataset,
    ):
        execute_statement.return_value = None
        extract_countries_and_territories_reference_dataset.return_value = (
            'countries_and_territories_reference_dataset'
        )
        extract_datahub_company_dataset.return_value = 'datahub_company_dataset'
        extract_datahub_export_to_countries.return_value = 'datahub_export_to_countries'
        extract_datahub_future_interest_countries.return_value = (
            'datahub_future_interest_countries'
        )
        extract_datahub_interactions.return_value = 'datahub_interaction'
        extract_datahub_omis.return_value = 'datahub_omis'

        populate_standardised_countries_task.return_value = (
            'populate_standardised_countries'
        )
        populate_interactions_analysed_task.return_value = (
            'populate_interactions_analysed'
        )
        populate_export_to_countries_task.return_value = 'populate_export_to_countries'
        populate_countries_and_sectors_of_interest_task.return_value = (
            'populate_countries_and_sectors_of_interest'
        )
        populate_countries_of_interest_task.return_value = (
            'populate_countries_of_interest'
        )
        populate_sectors_of_interest_task.return_value = 'populate_sectors_of_interest'
        populate_mentioned_in_interactions_task.return_value = (
            'populate_mentioned_in_interactions'
        )

        output = populate_database(True, [], [])

        assert extract_countries_and_territories_reference_dataset.called is True
        assert extract_datahub_company_dataset.called is True
        assert extract_datahub_export_to_countries.called is True
        assert extract_datahub_future_interest_countries.called is True
        assert extract_datahub_interactions.called is True
        assert extract_datahub_omis.called is True

        assert populate_standardised_countries_task.called is True
        assert populate_interactions_analysed_task.called is True
        assert populate_export_to_countries_task.called is True
        assert populate_countries_and_sectors_of_interest_task.called is True
        assert populate_countries_of_interest_task.called is True
        assert populate_sectors_of_interest_task.called is True
        assert populate_mentioned_in_interactions_task.called is True

        assert output == {
            'output': [
                'countries_and_territories_reference_dataset',
                'datahub_company_dataset',
                'datahub_export_to_countries',
                'datahub_interaction',
                'datahub_future_interest_countries',
                'datahub_omis',
                'populate_standardised_countries',
                'populate_interactions_analysed',
                'populate_export_to_countries',
                'populate_countries_and_sectors_of_interest',
                'populate_countries_of_interest',
                'populate_sectors_of_interest',
                'populate_mentioned_in_interactions',
            ]
        }

    @patch('app.etl.tasks.execute_statement')
    def test_only_specified_tasks_are_run(
        self,
        execute_statement,
        populate_mentioned_in_interactions_task,
        populate_sectors_of_interest_task,
        populate_countries_of_interest_task,
        populate_countries_and_sectors_of_interest_task,
        populate_export_to_countries_task,
        populate_interactions_analysed_task,
        populate_standardised_countries_task,
        extract_datahub_omis,
        extract_datahub_interactions,
        extract_datahub_future_interest_countries,
        extract_datahub_export_to_countries,
        extract_datahub_company_dataset,
        extract_countries_and_territories_reference_dataset,
    ):
        execute_statement.return_value = None
        extract_countries_and_territories_reference_dataset.return_value = (
            'countries_and_territories_reference_dataset'
        )
        extract_datahub_company_dataset.return_value = 'datahub_company_dataset'
        extract_datahub_export_to_countries.return_value = 'datahub_export_to_countries'
        extract_datahub_future_interest_countries.return_value = (
            'datahub_future_interest_countries'
        )
        extract_datahub_interactions.return_value = 'datahub_interaction'
        extract_datahub_omis.return_value = 'datahub_omis'

        populate_standardised_countries_task.return_value = (
            'populate_standardised_countries'
        )
        populate_interactions_analysed_task.return_value = (
            'populate_interactions_analysed'
        )
        populate_export_to_countries_task.return_value = 'populate_export_to_countries'
        populate_countries_and_sectors_of_interest_task.return_value = (
            'populate_countries_and_sectors_of_interest'
        )
        populate_countries_of_interest_task.return_value = (
            'populate_countries_of_interest'
        )
        populate_sectors_of_interest_task.return_value = 'populate_sectors_of_interest'
        populate_mentioned_in_interactions_task.return_value = (
            'populate_mentioned_in_interactions'
        )

        output = populate_database(True, ['datahub_company'], ['countries_of_interest'])

        assert extract_countries_and_territories_reference_dataset.called is False
        assert extract_datahub_company_dataset.called is True
        assert extract_datahub_export_to_countries.called is False
        assert extract_datahub_future_interest_countries.called is False
        assert extract_datahub_interactions.called is False
        assert extract_datahub_omis.called is False

        assert populate_standardised_countries_task.called is False
        assert populate_interactions_analysed_task.called is False
        assert populate_export_to_countries_task.called is False
        assert populate_countries_and_sectors_of_interest_task.called is False
        assert populate_countries_of_interest_task.called is True
        assert populate_sectors_of_interest_task.called is False
        assert populate_mentioned_in_interactions_task.called is False

        assert output == {
            'output': ['datahub_company_dataset', 'populate_countries_of_interest']
        }

    @patch('app.etl.tasks.datetime')
    def test_updates_task_status_to_success(
        self,
        mock_datetime,
        populate_mentioned_in_interactions_task,
        populate_sectors_of_interest_task,
        populate_countries_of_interest_task,
        populate_countries_and_sectors_of_interest_task,
        populate_export_to_countries_task,
        populate_interactions_analysed_task,
        populate_standardised_countries_task,
        extract_datahub_omis,
        extract_datahub_interactions,
        extract_datahub_future_interest_countries,
        extract_datahub_export_to_countries,
        extract_datahub_company_dataset,
        extract_countries_and_territories_reference_dataset,
        app_with_db,
    ):
        mock_datetime.datetime.now.return_value = datetime.datetime(2019, 1, 1, 2)
        sql = (
            "insert into etl_status (status, timestamp) values"
            "('RUNNING', '2019-01-01 01:00')"
        )
        execute_statement(sql)
        populate_database(True, [], [])
        sql = 'select * from etl_status'
        rows = execute_query(sql, df=False)
        assert len(rows) == 1
        assert rows[0][1] == 'SUCCESS'
        assert rows[0][2] == datetime.datetime(2019, 1, 1, 2)

        extract_countries_and_territories_reference_dataset.assert_called_once()
        extract_datahub_company_dataset.assert_called_once()
        extract_datahub_export_to_countries.assert_called_once()
        extract_datahub_future_interest_countries.assert_called_once()
        extract_datahub_interactions.assert_called_once()
        extract_datahub_omis.assert_called_once()

        populate_standardised_countries_task.assert_called_once_with()
        populate_interactions_analysed_task.assert_called_once_with()
        populate_export_to_countries_task.assert_called_once_with()
        populate_countries_and_sectors_of_interest_task.assert_called_once_with()
        populate_countries_of_interest_task.assert_called_once_with()
        populate_sectors_of_interest_task.assert_called_once_with()
        populate_mentioned_in_interactions_task.assert_called_once_with()
