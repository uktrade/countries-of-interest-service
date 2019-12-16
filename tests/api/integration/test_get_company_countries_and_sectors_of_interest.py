import pytest

import tests.api.utils as utils


@pytest.fixture(scope='module', autouse=True)
def setup_function(app, add_countries_and_sectors_of_interest):
    app.config['access_control']['hawk_enabled'] = False
    add_countries_and_sectors_of_interest(
        [
            {
                'company_id': '1',
                'country_of_interest': 'country1',
                'sector_of_interest': 'sector1',
                'source': 'source1',
                'source_id': 'source_id',
                'timestamp': '2009-10-10 12:12:12',
            }
        ]
    )


def test_get_company_countries_and_sectors_of_interest(app):
    url = 'http://localhost:80/api/v1/get-company-countries-and-sectors-of-interest'
    with app.test_client() as app_context:
        utils.assert_api_response(
            app_context=app_context,
            api=url,
            params='company-id=1',
            expected_response=(
                200,
                {
                    'headers': [
                        'companyId',
                        'countryOfInterest',
                        'sectorOfInterest',
                        'source',
                        'sourceId',
                        'timestamp',
                    ],
                    'next': None,
                    'values': [
                        [
                            '1',
                            'country1',
                            'sector1',
                            'source1',
                            'source_id',
                            '2009-10-10T12:12:12',
                        ]
                    ],
                },
            ),
        )
