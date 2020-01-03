import pytest

import tests.api.utils as utils

interest_1 = {
    'company_id': '1',
    'export_country': 'country1',
    'source': 'source1',
    'source_id': 'source_id',
    'timestamp': '2009-10-10 12:12:12',
}

interest_2 = {
    'company_id': '2',
    'country_of_interest': 'country2',
    'source': 'source2',
    'source_id': 'source_id2',
    'timestamp': '2009-10-10 12:12:12',
}


@pytest.fixture(scope='module', autouse=True)
def setup_function(app, add_company_export_countries):
    app.config['access_control']['hawk_enabled'] = False
    add_company_export_countries([interest_1, interest_2])


def test_get_company_export_countries(app):
    with app.test_client() as app_context:
        utils.assert_api_response(
            app_context=app_context,
            api='http://localhost:80/api/v1/get-company-export-countries',
            params='company-id=1',
            expected_response=(
                200,
                {
                    'headers': [
                        'companyId',
                        'countryOfInterest',
                        'source',
                        'sourceId',
                        'timestamp',
                    ],
                    'next': None,
                    'values': [
                        ['1', 'country1', 'source1', 'source_id', '2009-10-10T12:12:12']
                    ],
                },
            ),
        )


def test_multiple_company_filter(app):
    with app.test_client() as app_context:
        utils.assert_api_response(
            app_context=app_context,
            api='http://localhost:80/api/v1/get-company-export-countries',
            params='company-id=1&company-id=2',
            expected_response=(
                200,
                {
                    'headers': [
                        'companyId',
                        'countryOfInterest',
                        'source',
                        'sourceId',
                        'timestamp',
                    ],
                    'next': None,
                    'values': [
                        [
                            '1',
                            'country1',
                            'source1',
                            'source_id',
                            '2009-10-10T12:12:12',
                        ],
                        [
                            '2',
                            'country2',
                            'source2',
                            'source_id2',
                            '2009-10-10T12:12:12',
                        ],
                    ],
                },
            ),
        )


def test_country_filter(app):
    with app.test_client() as app_context:
        utils.assert_api_response(
            app_context=app_context,
            api='http://localhost:80/api/v1/get-company-export-countries',
            params='country=country1',
            expected_response=(
                200,
                {
                    'headers': [
                        'companyId',
                        'countryOfInterest',
                        'source',
                        'sourceId',
                        'timestamp',
                    ],
                    'next': None,
                    'values': [
                        ['1', 'country1', 'source1', 'source_id', '2009-10-10T12:12:12']
                    ],
                },
            ),
        )


def test_multiple_country_filter(app):
    with app.test_client() as app_context:
        utils.assert_api_response(
            app_context=app_context,
            api='http://localhost:80/api/v1/get-company-export-countries',
            params='country=country1&country=country2',
            expected_response=(
                200,
                {
                    'headers': [
                        'companyId',
                        'countryOfInterest',
                        'source',
                        'sourceId',
                        'timestamp',
                    ],
                    'next': None,
                    'values': [
                        [
                            '1',
                            'country1',
                            'source1',
                            'source_id',
                            '2009-10-10T12:12:12',
                        ],
                        [
                            '2',
                            'country2',
                            'source2',
                            'source_id2',
                            '2009-10-10T12:12:12',
                        ],
                    ],
                },
            ),
        )


def test_single_source_filter(app):
    url = 'http://localhost:80/api/v1/get-company-export-countries'
    with app.test_client() as app_context:
        utils.assert_api_response(
            app_context=app_context,
            api=url,
            params='source=source2',
            expected_response=(
                200,
                {
                    'headers': [
                        'companyId',
                        'countryOfInterest',
                        'source',
                        'sourceId',
                        'timestamp',
                    ],
                    'next': None,
                    'values': [
                        [
                            '2',
                            'country2',
                            'source2',
                            'source_id2',
                            '2009-10-10T12:12:12',
                        ]
                    ],
                },
            ),
        )


def test_multiple_source_filter(app):
    url = 'http://localhost:80/api/v1/get-company-export-countries'
    with app.test_client() as app_context:
        utils.assert_api_response(
            app_context=app_context,
            api=url,
            params='source=source2&source=source1',
            expected_response=(
                200,
                {
                    'headers': [
                        'companyId',
                        'countryOfInterest',
                        'source',
                        'sourceId',
                        'timestamp',
                    ],
                    'next': None,
                    'values': [
                        [
                            '1',
                            'country1',
                            'source1',
                            'source_id',
                            '2009-10-10T12:12:12',
                        ],
                        [
                            '2',
                            'country2',
                            'source2',
                            'source_id2',
                            '2009-10-10T12:12:12',
                        ],
                    ],
                },
            ),
        )


def test_pagination(app):
    url = 'http://localhost:80/api/v1/get-company-export-countries'
    with app.test_client() as app_context:
        utils.assert_api_response(
            app_context=app_context,
            api=url,
            params='next-source=source1&next-source-id=source_id_2',
            expected_response=(
                200,
                {
                    'headers': [
                        'companyId',
                        'countryOfInterest',
                        'source',
                        'sourceId',
                        'timestamp',
                    ],
                    'next': None,
                    'values': [
                        [
                            '2',
                            'country2',
                            'source2',
                            'source_id2',
                            '2009-10-10T12:12:12',
                        ]
                    ],
                },
            ),
        )


def test_pagination_next(app):
    app.config['app']['pagination_size'] = 1
    url = 'http://localhost:80/api/v1/get-company-export-countries'
    with app.test_client() as app_context:
        utils.assert_api_response(
            app_context=app_context,
            api=url,
            params='',
            expected_response=(
                200,
                {
                    'headers': [
                        'companyId',
                        'countryOfInterest',
                        'source',
                        'sourceId',
                        'timestamp',
                    ],
                    'next': 'http://localhost/api/v1/'
                    'get-company-export-countries?'
                    'next-source=source2&next-source-id=source_id2',
                    'values': [
                        ['1', 'country1', 'source1', 'source_id', '2009-10-10T12:12:12']
                    ],
                },
            ),
        )
