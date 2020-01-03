import pytest

import tests.api.utils as utils


interest_1 = {
    'company_id': '1',
    'country_of_interest': 'country1',
    'standardised_country': 'standardised_country1',
    'source': 'source1',
    'source_id': 'source_id',
    'timestamp': '2009-10-10T12:12:12',
}

interest_2 = {
    'company_id': '2',
    'country_of_interest': 'country2',
    'standardised_country': 'standardised_country2',
    'source': 'source2',
    'source_id': 'source_id2',
    'timestamp': '2009-10-10T12:12:12',
}

@pytest.fixture(scope='function', autouse=True)
def setup_function(app, add_company_countries_of_interest):
    app.config['access_control']['hawk_enabled'] = False
    old_pagination_size = app.config['app']['pagination_size']
    add_company_countries_of_interest([interest_1, interest_2])
    yield
    app.config['app']['pagination_size'] = old_pagination_size


def test_single_company_filter(app):
    with app.test_client() as app_context:
        utils.assert_api_response(
            app_context=app_context,
            api='http://localhost:80/api/v1/get-company-countries-of-interest',
            params='company-id=1',
            expected_response=(
                200,
                {
                    'headers': [
                        'companyId',
                        'countryOfInterest',
                        'standardisedCountry',
                        'source',
                        'sourceId',
                        'timestamp',
                    ],
                    'next': None,
                    'values': [list(interest_1.values())],
                },
            ),
        )


def test_multiple_company_filter(app):
    with app.test_client() as app_context:
        utils.assert_api_response(
            app_context=app_context,
            api='http://localhost:80/api/v1/get-company-countries-of-interest',
            params='company-id=1&company-id=2',
            expected_response=(
                200,
                {
                    'headers': [
                        'companyId',
                        'countryOfInterest',
                        'standardisedCountry',
                        'source',
                        'sourceId',
                        'timestamp',
                    ],
                    'next': None,
                    'values': [
                        list(interest_1.values()),
                        list(interest_2.values())
                    ],
                },
            ),
        )


def test_single_country_filter(app):
    with app.test_client() as app_context:
        utils.assert_api_response(
            app_context=app_context,
            api='http://localhost:80/api/v1/get-company-countries-of-interest',
            params='country=country1',
            expected_response=(
                200,
                {
                    'headers': [
                        'companyId',
                        'countryOfInterest',
                        'standardisedCountry',
                        'source',
                        'sourceId',
                        'timestamp',
                    ],
                    'next': None,
                    'values': [list(interest_1.values())],
                },
            ),
        )


def test_multiple_country_filter(app):
    with app.test_client() as app_context:
        utils.assert_api_response(
            app_context=app_context,
            api='http://localhost:80/api/v1/get-company-countries-of-interest',
            params='country=country1&country=country2',
            expected_response=(
                200,
                {
                    'headers': [
                        'companyId',
                        'countryOfInterest',
                        'standardisedCountry',
                        'source',
                        'sourceId',
                        'timestamp',
                    ],
                    'next': None,
                    'values': [list(interest_1.values()), list(interest_2.values())],
                },
            ),
        )


def test_single_source_filter(app):
    url = 'http://localhost:80/api/v1/get-company-countries-of-interest'
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
                        'standardisedCountry',
                        'source',
                        'sourceId',
                        'timestamp',
                    ],
                    'next': None,
                    'values': [list(interest_2.values())],
                },
            ),
        )


def test_multiple_source_filter(app):
    url = 'http://localhost:80/api/v1/get-company-countries-of-interest'
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
                        'standardisedCountry',
                        'source',
                        'sourceId',
                        'timestamp',
                    ],
                    'next': None,
                    'values': [list(interest_1.values()), list(interest_2.values())],
                },
            ),
        )


def test_pagination(app):
    url = 'http://localhost:80/api/v1/get-company-countries-of-interest'
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
                        'standardisedCountry',
                        'source',
                        'sourceId',
                        'timestamp',
                    ],
                    'next': None,
                    'values': [list(interest_2.values())],
                },
            ),
        )


def test_pagination_next(app):
    app.config['app']['pagination_size'] = 1
    url = 'http://localhost:80/api/v1/get-company-countries-of-interest'
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
                        'standardisedCountry',
                        'source',
                        'sourceId',
                        'timestamp',
                    ],
                    'next': 'http://localhost/api/v1/'
                    'get-company-countries-of-interest?'
                    'next-source=source2&next-source-id=source_id2',
                    'values': [list(interest_1.values())],
                },
            ),
        )
