import pytest

import tests.api.utils as utils


mention_1 = {
    'id': 1,
    'company_id': 'company_1',
    'country_of_interest': 'country_1',
    'interaction_id': '6387f34b-649f-4b51-99d4-2e013b055e61',
    'timestamp': '2020-01-01T00:00:00'
}

mention_2 = {
    'id': 2,
    'company_id': 'company_2',
    'country_of_interest': 'country_2',
    'interaction_id': '6387f34b-649f-4b51-99d4-2e013b055e62',
    'timestamp': '2020-01-02T00:00:00'
}


@pytest.fixture(scope='function', autouse=True)
def setup_function(app, add_company_countries_of_interest, add_mentioned_in_interactions):
    app.config['access_control']['hawk_enabled'] = False
    old_pagination_size = app.config['app']['pagination_size']
    add_mentioned_in_interactions([mention_1, mention_2])
    yield
    app.config['app']['pagination_size'] = old_pagination_size


def test_single_company_filter(app):

    print('list(mention_1.values()):', list(mention_1.values()))
    with app.test_client() as app_context:
        utils.assert_api_response(
            app_context=app_context,
            api='http://localhost:80/api/v1/get-company-countries-mentioned-in-interactions',
            params='company-id=company_1',
            expected_response=(
                200,
                {
                    'headers': [
                        'companyId',
                        'countryOfInterest',
                        'interactionId',
                        'timestamp',
                    ],
                    'next': None,
                    'values': [list(mention_1.values())[1:]],
                },
            ),
        )


def test_multiple_company_filter(app):
    with app.test_client() as app_context:
        utils.assert_api_response(
            app_context=app_context,
            api='http://localhost:80/api/v1/get-company-countries-mentioned-in-interactions',
            params='company-id=company_1&company-id=company_2',
            expected_response=(
                200,
                {
                    'headers': [
                        'companyId',
                        'countryOfInterest',
                        'interactionId',
                        'timestamp',
                    ],
                    'next': None,
                    'values': [list(mention_1.values())[1:], list(mention_2.values())[1:]],
                },
            ),
        )


def test_single_country_filter(app):
    with app.test_client() as app_context:
        utils.assert_api_response(
            app_context=app_context,
            api='http://localhost:80/api/v1/get-company-countries-mentioned-in-interactions',
            params='country=country_1',
            expected_response=(
                200,
                {
                    'headers': [
                        'companyId',
                        'countryOfInterest',
                        'interactionId',
                        'timestamp',
                    ],
                    'next': None,
                    'values': [list(mention_1.values())[1:]],
                },
            ),
        )


def test_multiple_country_filter(app):
    with app.test_client() as app_context:
        utils.assert_api_response(
            app_context=app_context,
            api='http://localhost:80/api/v1/get-company-countries-mentioned-in-interactions',
            params='country=country_1&country=country_2',
            expected_response=(
                200,
                {
                    'headers': [
                        'companyId',
                        'countryOfInterest',
                        'interactionId',
                        'timestamp',
                    ],
                    'next': None,
                    'values': [list(mention_1.values())[1:], list(mention_2.values())[1:]],
                },
            ),
        )


def test_pagination(app):
    url = 'http://localhost:80/api/v1/get-company-countries-mentioned-in-interactions'
    with app.test_client() as app_context:
        utils.assert_api_response(
            app_context=app_context,
            api=url,
            params='next-id=2',
            expected_response=(
                200,
                {
                    'headers': [
                        'companyId',
                        'countryOfInterest',
                        'interactionId',
                        'timestamp',
                    ],
                    'next': None,
                    'values': [list(mention_2.values())[1:]],
                },
            ),
        )


def test_pagination_next(app):
    app.config['app']['pagination_size'] = 1
    url = 'http://localhost:80/api/v1/get-company-countries-mentioned-in-interactions'
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
                        'interactionId',
                        'timestamp',
                    ],
                    'next': 'http://localhost/api/v1/'
                    'get-company-countries-mentioned-in-interactions?'
                    'orientation=tabular&'
                    'next-id=2',
                    'values': [list(mention_1.values())[1:]],
                },
            ),
        )
