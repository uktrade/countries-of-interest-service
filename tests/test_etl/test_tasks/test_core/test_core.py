import datetime
from unittest.mock import patch

from app import app

from db import get_db

from etl.tasks.core import populate_database

from tests.TestCase import TestCase


@patch('etl.tasks.core.extract_datahub_company_dataset')
@patch('etl.tasks.core.extract_datahub_export_countries')
@patch('etl.tasks.core.extract_datahub_future_interest_countries')
@patch('etl.tasks.core.extract_datahub_interactions')
@patch('etl.tasks.core.extract_datahub_omis')
@patch('etl.tasks.core.extract_datahub_sectors')
@patch('etl.tasks.core.extract_export_wins')
@patch('etl.tasks.core.ExportCountriesTask')
@patch('etl.tasks.core.PopulateCountriesAndSectorsOfInterestTask')
@patch('etl.tasks.core.PopulateCountriesOfInterestTask')
@patch('etl.tasks.core.SectorsOfInterestTask')
class TestPopulateDatabase(TestCase):
    @patch('etl.tasks.core.get_db')
    def test_tasks_are_run(
        self,
        get_db,
        SectorsOfInterestTask,
        PopulateCountriesOfInterestTask,
        PopulateCountriesAndSectorsOfInterestTask,
        ExportCountriesTask,
        extract_export_wins,
        extract_datahub_sectors,
        extract_datahub_omis,
        extract_datahub_interactions,
        extract_datahub_future_interest_countries,
        extract_datahub_export_countries,
        extract_datahub_company_dataset,
    ):
        with app.app_context():
            output = populate_database(drop_table=True)

        db_context = get_db.return_value.__enter__.return_value
        extract_datahub_company_dataset.assert_called_once()
        extract_datahub_export_countries.assert_called_once()
        extract_datahub_future_interest_countries.assert_called_once()
        extract_datahub_interactions.assert_called_once()
        extract_datahub_omis.assert_called_once()
        extract_datahub_sectors.assert_called_once()
        extract_export_wins.assert_called_once()
        ExportCountriesTask.assert_called_once_with(
            connection=db_context, drop_table=True
        )
        ExportCountriesTask.return_value.assert_called_once()
        PopulateCountriesAndSectorsOfInterestTask.assert_called_once_with(
            connection=db_context, drop_table=True
        )
        (PopulateCountriesAndSectorsOfInterestTask.return_value.assert_called_once())
        PopulateCountriesOfInterestTask.assert_called_once_with(
            connection=db_context, drop_table=True
        )
        PopulateCountriesOfInterestTask.return_value.assert_called_once()
        SectorsOfInterestTask.assert_called_once_with(
            connection=db_context, drop_table=True
        )
        SectorsOfInterestTask.return_value.assert_called_once()

        expected_output = {
            'output': [
                extract_datahub_company_dataset.return_value,
                extract_datahub_export_countries.return_value,
                extract_datahub_interactions.return_value,
                extract_datahub_future_interest_countries.return_value,
                extract_datahub_omis.return_value,
                extract_datahub_sectors.return_value,
                extract_export_wins.return_value,
                ExportCountriesTask.return_value.return_value,
                (PopulateCountriesAndSectorsOfInterestTask.return_value.return_value),
                PopulateCountriesOfInterestTask.return_value.return_value,
                SectorsOfInterestTask.return_value.return_value,
            ]
        }

        self.assertEqual(output, expected_output)

    @patch('etl.tasks.core.datetime')
    def test_updates_task_status_to_success(
        self,
        mock_datetime,
        SectorsOfInterestTask,
        PopulateCountriesOfInterestTask,
        PopulateCountriesAndSectorsOfInterestTask,
        ExportCountriesTask,
        extract_export_wins,
        extract_datahub_sectors,
        extract_datahub_omis,
        extract_datahub_interactions,
        extract_datahub_future_interest_countries,
        extract_datahub_export_countries,
        extract_datahub_company_dataset,
    ):

        mock_datetime.datetime.now.return_value = datetime.datetime(2019, 1, 1, 2)
        with app.app_context():
            with get_db() as connection:
                with connection.cursor() as cursor:
                    sql = (
                        'create table if not exists etl_status ('
                        'status varchar(100), timestamp timestamp)'
                    )
                    cursor.execute(sql)
                    sql = (
                        "insert into etl_status values"
                        "('RUNNING', '2019-01-01 01:00')"
                    )
                    cursor.execute(sql)
            populate_database(drop_table=True)
            with get_db() as connection:
                with connection.cursor() as cursor:
                    sql = 'select * from etl_status'
                    cursor.execute(sql)
                    rows = cursor.fetchall()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows, [('SUCCESS', datetime.datetime(2019, 1, 1, 2))])
