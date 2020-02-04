from unittest import mock

import pytest

import tests.api.utils as utils

entry_1 = {
    'service_company_id': '1',
    'company_match_id': 1,
    'country': 'country1',
    'sector': 'sector1',
    'type': 'interested',
    'service': 'datahub',
    'source': 'source1',
    'source_id': 'source_id_1',
    'timestamp': '2009-10-10T12:12:12',
}

entry_2 = {
    'service_company_id': '2',
    'company_match_id': 2,
    'country': 'country2',
    'sector': None,
    'type': 'exported',
    'service': 'datahub',
    'source': 'source2',
    'source_id': 'source_id_2',
    'timestamp': '2009-10-10T13:00:00',
}

entry_3 = {
    'service_company_id': '3',
    'company_match_id': 3,
    'country': 'country3',
    'sector': 'sector3',
    'type': 'interested',
    'service': 'export-wins',
    'source': 'source3',
    'source_id': 'source_id_1',
    'timestamp': '2009-10-11T13:00:00',
}

entry_4 = {
    'service_company_id': '4',
    'company_match_id': 4,
    'country': 'country4',
    'sector': 'sector4',
    'type': 'mentioned',
    'service': 'datahub',
    'source': 'interactions',
    'source_id': 'source_id_4',
    'timestamp': '2009-10-11T14:00:00',
}


@pytest.fixture(scope='module', autouse=True)
def setup_function(app, add_countries_and_sectors_of_interest):
    add_countries_and_sectors_of_interest([entry_1, entry_2, entry_3, entry_4])


def test_get_countries(app, sso_authenticated_request):
    url = 'http://localhost:80/api/v1/get-data-visualisation-data/country'
    with app.test_client() as app_context:
        utils.assert_api_response(
            app_context=app_context,
            api=url,
            params='exporter-status=interested',
            expected_response=(
                200,
                {
                    'nInterests': [
                        {
                            'country': 'country1',
                            'date': '2009-10-01T00:00:00',
                            'nInterests': 1,
                            'nInterestsCumulative': 1,
                            'shareOfInterest': 0.5,
                            'shareOfInterestCumulative': 0.5,
                        },
                        {
                            'country': 'country3',
                            'date': '2009-10-01T00:00:00',
                            'nInterests': 1,
                            'nInterestsCumulative': 1,
                            'shareOfInterest': 0.5,
                            'shareOfInterestCumulative': 0.5,
                        },
                    ],
                    'top': ['country1', 'country3'],
                },
            ),
        )


def test_get_sectors(app):
    with mock.patch('app.sso.token.is_authenticated') as mock_is_authenticated:
        mock_is_authenticated.return_value = True
        url = 'http://localhost:80/api/v1/get-data-visualisation-data/sector'
        with app.test_client() as app_context:
            utils.assert_api_response(
                app_context=app_context,
                api=url,
                params='exporter-status=interested',
                expected_response=(
                    200,
                    {
                        'nInterests': [
                            {
                                'date': '2009-10-01T00:00:00',
                                'nInterests': 1,
                                'nInterestsCumulative': 1,
                                'sector': 'sector1',
                                'shareOfInterest': 0.5,
                                'shareOfInterestCumulative': 0.5,
                            },
                            {
                                'date': '2009-10-01T00:00:00',
                                'nInterests': 1,
                                'nInterestsCumulative': 1,
                                'sector': 'sector3',
                                'shareOfInterest': 0.5,
                                'shareOfInterestCumulative': 0.5,
                            },
                        ],
                        'top': ['sector1', 'sector3'],
                    },
                ),
            )


def test_get_countries_mentioned(app, sso_authenticated_request):
    url = 'http://localhost:80/api/v1/get-data-visualisation-data/country'
    with app.test_client() as app_context:
        utils.assert_api_response(
            app_context=app_context,
            api=url,
            params='exporter-status=mentioned',
            expected_response=(
                200,
                {
                    'nInterests': [
                        {
                            'country': 'country4',
                            'date': '2009-10-01T00:00:00',
                            'nInterests': 1,
                            'nInterestsCumulative': 1,
                            'shareOfInterest': 1.0,
                            'shareOfInterestCumulative': 1.0,
                        },
                    ],
                    'top': ['country4'],
                },
            ),
        )


def test_invalid_exporter_status(app, sso_authenticated_request):
    url = 'http://localhost:80/api/v1/get-data-visualisation-data/country'
    with app.test_client() as app_context:
        utils.assert_api_response(
            app_context=app_context,
            api=url,
            params='exporter-status=invalid',
            expected_response=(400, None),
        )


def test_invalid_fields_status(app, sso_authenticated_request):
    url = 'http://localhost:80/api/v1/get-data-visualisation-data/invalid'
    with app.test_client() as app_context:
        utils.assert_api_response(
            app_context=app_context,
            api=url,
            params='exporter-status=invalid',
            expected_response=(400, None),
        )


def test_invalid_sector_exporter_status(app, sso_authenticated_request):
    url = 'http://localhost:80/api/v1/get-data-visualisation-data/sector'
    with app.test_client() as app_context:
        utils.assert_api_response(
            app_context=app_context,
            api=url,
            params='exporter-status=mentioned',
            expected_response=(400, None),
        )
