from tests.TestCase import TestCase
from unittest.mock import Mock, patch
from etl.views import populate_database

@patch('etl.views.extract_datahub_company_dataset')
@patch('etl.views.extract_datahub_export_countries')
@patch('etl.views.extract_datahub_future_interest_countries')
@patch('etl.views.extract_datahub_interactions')
@patch('etl.views.extract_datahub_omis_dataset')
@patch('etl.views.extract_datahub_sectors')
@patch('etl.views.extract_export_wins')
@patch('etl.views.DatahubCompanyIDToCompaniesHouseCompanyNumberTask')
@patch('etl.views.ExportCountriesTask')
@patch('etl.views.PopulateCountriesAndSectorsOfInterestTask')
@patch('etl.views.PopulateCountriesOfInterestTask')
@patch('etl.views.SectorsOfInterestTask')
@patch('etl.views.get_db')
@patch('etl.views.request')
class TestPopulateDatabase(TestCase):

    def test_datahub_company_dataset(
            self,
            request,
            get_db,
            SectorsOfInterestTask,
            PopulateCountriesOfInterestTask,
            PopulateCountriesAndSectorsOfInterestTask,
            ExportCountriesTask,
            DatahubCompanyIDToCompaniesHouseCompanyNumberTask,
            extract_export_wins,
            extract_datahub_sectors,
            extract_datahub_omis_dataset,
            extract_datahub_interactions,
            extract_datahub_future_interest_countries,
            extract_datahub_export_countries,
            extract_datahub_company_dataset,
    ):
        request.args = {'drop-table': ''}
        output = populate_database()
        
        extract_datahub_company_dataset.assert_called_once()
        extract_datahub_export_countries.assert_called_once()
        extract_datahub_future_interest_countries.assert_called_once()
        extract_datahub_interactions.assert_called_once()
        extract_datahub_omis_dataset.assert_called_once()
        extract_datahub_sectors.assert_called_once()
        extract_export_wins.assert_called_once()
        DatahubCompanyIDToCompaniesHouseCompanyNumberTask.assert_called_once_with(
            connection=get_db.return_value,
            drop_table=True
        )
        DatahubCompanyIDToCompaniesHouseCompanyNumberTask.return_value.assert_called_once()
        ExportCountriesTask.assert_called_once_with(
            connection=get_db.return_value,
            drop_table=True
        )
        ExportCountriesTask.return_value.assert_called_once()
        PopulateCountriesAndSectorsOfInterestTask.assert_called_once_with(
            connection=get_db.return_value,
            drop_table=True
        )
        PopulateCountriesAndSectorsOfInterestTask.return_value.assert_called_once()
        PopulateCountriesOfInterestTask.assert_called_once_with(
            connection=get_db.return_value,
            drop_table=True
        )
        PopulateCountriesOfInterestTask.return_value.assert_called_once()
        SectorsOfInterestTask.assert_called_once_with(
            connection=get_db.return_value,
            drop_table=True
        )
        SectorsOfInterestTask.return_value.assert_called_once()

        expected_output = {
            'output': [
                extract_datahub_company_dataset.return_value,
                extract_datahub_export_countries.return_value,
                extract_datahub_interactions.return_value,
                extract_datahub_future_interest_countries.return_value,
                extract_datahub_omis_dataset.return_value,
                extract_datahub_sectors.return_value,
                extract_export_wins.return_value,
                DatahubCompanyIDToCompaniesHouseCompanyNumberTask.return_value.return_value,
                ExportCountriesTask.return_value.return_value,
                PopulateCountriesAndSectorsOfInterestTask.return_value.return_value,
                PopulateCountriesOfInterestTask.return_value.return_value,
                SectorsOfInterestTask.return_value.return_value,
            ]
        }

        self.assertEqual(output, expected_output)
