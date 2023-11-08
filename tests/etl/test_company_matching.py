import requests
from app.common.tests.utils import rows_equal_query_results
from flask import current_app as flask_app

from app.db.models.internal import CountriesAndSectorsInterest
from app.etl.tasks.company_matching import Task


def test_company_matching(
    mocker,
    app,
    add_datahub_company,
    add_datahub_contact,
    add_export_wins,
    add_countries_and_sectors_of_interest,
):
    add_datahub_company(
        [
            {
                'company_name': 'company_name1',
                'datahub_company_id': 'f17645ba-c38e-4215-b09e-47451edb9125',
                'companies_house_id': '90821308',
                'sector': 'Food',
                'reference_code': 'ORG-01233',
                'postcode': 'GS3 3PL',
                'modified_on': '2018-01-01 00:00:00',
            },
            {
                'company_name': 'company_name2',
                'datahub_company_id': '5965b49e-dae5-4a06-81fa-c894e652cd52',
                'companies_house_id': '92890123',
                'sector': 'Retail',
                'reference_code': 'ORG-01244',
                'postcode': 'GH3 3PL',
                'modified_on': '2017-01-01 00:00:00',
            },
        ]
    )
    add_datahub_contact(
        [
            {
                'datahub_contact_id': '93041a63-57d3-4b6b-a83d-172ba854f770',
                'datahub_company_id': '5965b49e-dae5-4a06-81fa-c894e652cd52',
                'email': 'john@test.com',
            }
        ]
    )
    add_export_wins(
        [
            {
                'export_wins_company_id': 'ORG-01244',
                'contact_email_address': 'john@test.com',
                'sector': 'Aerospace',
                'company_name': 'company_name2',
                'country': 'China',
                'export_wins_id': 'ffa75985-7bc0-4e9f-8d58-28a7f234b7fc',
                'created_on': '2011-10-10 12:12:12',
                'date_won': '2010-11-11 12:12:12',
            },
        ]
    )
    add_countries_and_sectors_of_interest(
        [
            {
                'service_company_id': 'f17645ba-c38e-4215-b09e-47451edb9125',
                'company_match_id': None,
                'country': 'country1',
                'sector': 'sector1',
                'type': 'interested',
                'service': 'datahub',
                'source': 'source1',
                'source_id': 'f17645ba-c38e-4215-b09e-47451edb9125',
                'timestamp': '2009-10-10T12:12:12',
            },
            {
                'service_company_id': '5965b49e-dae5-4a06-81fa-c894e652cd52',
                'company_match_id': None,
                'country': 'country2',
                'sector': None,
                'type': 'exported',
                'service': 'datahub',
                'source': 'source2',
                'source_id': '5965b49e-dae5-4a06-81fa-c894e652cd52',
                'timestamp': '2009-10-10T13:00:00',
            },
            {
                'service_company_id': 'ORG-01244',
                'company_match_id': None,
                'country': 'China',
                'sector': 'Aerospace',
                'type': 'exported',
                'service': 'export_wins',
                'source': 'export_wins',
                'source_id': 'ffa75985-7bc0-4e9f-8d58-28a7f234b7fc',
                'timestamp': '2010-11-11 12:12:12',
            },
        ],
        temp=True,
    )

    _api_mock(
        mocker,
        {
            "matches": [
                {
                    "id": 'f17645ba-c38e-4215-b09e-47451edb9125',
                    "match_id": 1,
                    "similarity": "101010",
                },
                {
                    "id": '5965b49e-dae5-4a06-81fa-c894e652cd52',
                    "match_id": 2,
                    "similarity": "101110",
                },
                {
                    "id": 'ffa75985-7bc0-4e9f-8d58-28a7f234b7fc',
                    "match_id": 2,
                    "similarity": "001110",
                },
            ]
        },
        200,
    )
    app.config['cms']['base_url'] = 'https://test.com'
    task = Task()
    task()

    expected_rows = [
        (
            1,
            'f17645ba-c38e-4215-b09e-47451edb9125',
            1,
            'country1',
            'sector1',
            'interested',
            'datahub',
            'source1',
            'f17645ba-c38e-4215-b09e-47451edb9125',
            '2009-10-10 12:12:12',
        ),
        (
            2,
            '5965b49e-dae5-4a06-81fa-c894e652cd52',
            2,
            'country2',
            None,
            'exported',
            'datahub',
            'source2',
            '5965b49e-dae5-4a06-81fa-c894e652cd52',
            '2009-10-10 13:00:00',
        ),
        (
            3,
            'ORG-01244',
            2,
            'China',
            'Aerospace',
            'exported',
            'export_wins',
            'export_wins',
            'ffa75985-7bc0-4e9f-8d58-28a7f234b7fc',
            '2010-11-11 12:12:12',
        ),
    ]

    assert rows_equal_query_results(
        flask_app.dbi,
        expected_rows,
        f'SELECT * FROM {CountriesAndSectorsInterest.get_fq_table_name()}',
    )


def _api_mock(mocker, json_data, status_code):
    def return_json(url, headers=None, json=None):
        class MockJsonResponse:
            def __init__(self):
                self.url = url
                self.status_code = status_code
                self.json_data = json_data
                self.text = json_data

            def json(self):
                return self.json_data

        return MockJsonResponse()

    retrieved_json_mock = mocker.patch.object(requests, 'post', side_effect=return_json)
    return retrieved_json_mock
