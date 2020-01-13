import pytest

import tests.api.utils as utils

interest_1 = {
    'company_id': '1',
    'country_of_interest': 'country1',
    'standardised_country': 'country1',
    'sector_of_interest': 'sector1',
    'source': 'source1',
    'source_id': 'source_id_1',
    'timestamp': '2009-10-10T12:12:12',
}

interest_2 = {
    'company_id': '2',
    'country_of_interest': 'country2',
    'standardised_country': 'country2',
    'sector_of_interest': 'sector2',
    'source': 'source2',
    'source_id': 'source_id',
    'timestamp': '2009-10-10T13:00:00',
}


@pytest.fixture(scope='function', autouse=True)
def setup_function(app, add_countries_and_sectors_of_interest):
    app.config['access_control']['hawk_enabled'] = False
    old_pagination_size = app.config['app']['pagination_size']
    add_countries_and_sectors_of_interest([interest_1, interest_2])
    yield
    app.config['app']['pagination_size'] = old_pagination_size


def test_single_company_id_filter(app):
    url = 'http://localhost:80/api/v1/get-company-countries-and-sectors-of-interest'
    with app.test_client() as app_context:
        utils.assert_api_response(
            app_context=app_context,
            api=url,
            params='company-id=2',
            expected_response=(
                200,
                {
                    'headers': [
                        'companyId',
                        'countryOfInterest',
                        'standardisedCountry',
                        'sectorOfInterest',
                        'source',
                        'sourceId',
                        'timestamp',
                    ],
                    'next': None,
                    'values': [list(interest_2.values())],
                },
            ),
        )


def test_multiple_company_id_filter(app):
    url = 'http://localhost:80/api/v1/get-company-countries-and-sectors-of-interest'
    with app.test_client() as app_context:
        utils.assert_api_response(
            app_context=app_context,
            api=url,
            params='company-id=2&company-id=1',
            expected_response=(
                200,
                {
                    'headers': [
                        'companyId',
                        'countryOfInterest',
                        'standardisedCountry',
                        'sectorOfInterest',
                        'source',
                        'sourceId',
                        'timestamp',
                    ],
                    'next': None,
                    'values': [list(interest_1.values()), list(interest_2.values())],
                },
            ),
        )


def test_single_country_filter(app):
    url = 'http://localhost:80/api/v1/get-company-countries-and-sectors-of-interest'
    with app.test_client() as app_context:
        utils.assert_api_response(
            app_context=app_context,
            api=url,
            params='country=country2',
            expected_response=(
                200,
                {
                    'headers': [
                        'companyId',
                        'countryOfInterest',
                        'standardisedCountry',
                        'sectorOfInterest',
                        'source',
                        'sourceId',
                        'timestamp',
                    ],
                    'next': None,
                    'values': [list(interest_2.values())],
                },
            ),
        )


def test_multiple_country_filter(app):
    url = 'http://localhost:80/api/v1/get-company-countries-and-sectors-of-interest'
    with app.test_client() as app_context:
        utils.assert_api_response(
            app_context=app_context,
            api=url,
            params='country=country2&country=country1',
            expected_response=(
                200,
                {
                    'headers': [
                        'companyId',
                        'countryOfInterest',
                        'standardisedCountry',
                        'sectorOfInterest',
                        'source',
                        'sourceId',
                        'timestamp',
                    ],
                    'next': None,
                    'values': [list(interest_1.values()), list(interest_2.values())],
                },
            ),
        )


def test_single_sector_filter(app):
    url = 'http://localhost:80/api/v1/get-company-countries-and-sectors-of-interest'
    with app.test_client() as app_context:
        utils.assert_api_response(
            app_context=app_context,
            api=url,
            params='sector=sector2',
            expected_response=(
                200,
                {
                    'headers': [
                        'companyId',
                        'countryOfInterest',
                        'standardisedCountry',
                        'sectorOfInterest',
                        'source',
                        'sourceId',
                        'timestamp',
                    ],
                    'next': None,
                    'values': [list(interest_2.values())],
                },
            ),
        )


def test_multiple_sector_filter(app):
    url = 'http://localhost:80/api/v1/get-company-countries-and-sectors-of-interest'
    with app.test_client() as app_context:
        utils.assert_api_response(
            app_context=app_context,
            api=url,
            params='sector=sector2&sector=sector1',
            expected_response=(
                200,
                {
                    'headers': [
                        'companyId',
                        'countryOfInterest',
                        'standardisedCountry',
                        'sectorOfInterest',
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
    url = 'http://localhost:80/api/v1/get-company-countries-and-sectors-of-interest'
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
                        'sectorOfInterest',
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
    url = 'http://localhost:80/api/v1/get-company-countries-and-sectors-of-interest'
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
                        'sectorOfInterest',
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
    url = 'http://localhost:80/api/v1/get-company-countries-and-sectors-of-interest'
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
                        'sectorOfInterest',
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
    url = 'http://localhost:80/api/v1/get-company-countries-and-sectors-of-interest'
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
                        'sectorOfInterest',
                        'source',
                        'sourceId',
                        'timestamp',
                    ],
                    'next': 'http://localhost/api/v1/'
                    'get-company-countries-and-sectors-of-interest?'
                    'next-source=source2&next-source-id=source_id',
                    'values': [list(interest_1.values())],
                },
            ),
        )
