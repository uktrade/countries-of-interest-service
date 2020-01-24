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


@pytest.fixture(scope='module', autouse=True)
def setup_function(app, add_countries_and_sectors_of_interest):
    app.config['access_control']['hawk_enabled'] = False
    add_countries_and_sectors_of_interest([entry_1, entry_2, entry_3])


def test_single_service_company_id_filter(app):
    url = 'http://localhost:80/api/v1/get-company-countries-and-sectors-of-interest'
    with app.test_client() as app_context:
        utils.assert_api_response(
            app_context=app_context,
            api=url,
            params='service-company-id=2',
            expected_response=(
                200,
                {
                    'headers': [
                        'serviceCompanyId',
                        'companyMatchId',
                        'country',
                        'sector',
                        'type',
                        'service',
                        'source',
                        'sourceId',
                        'timestamp',
                    ],
                    'next': None,
                    'values': [list(entry_2.values())],
                },
            ),
        )


def test_multiple_company_id_filter(app):
    url = 'http://localhost:80/api/v1/get-company-countries-and-sectors-of-interest'
    with app.test_client() as app_context:
        utils.assert_api_response(
            app_context=app_context,
            api=url,
            params='service-company-id=2&service-company-id=1',
            expected_response=(
                200,
                {
                    'headers': [
                        'serviceCompanyId',
                        'companyMatchId',
                        'country',
                        'sector',
                        'type',
                        'service',
                        'source',
                        'sourceId',
                        'timestamp',
                    ],
                    'next': None,
                    'values': [list(entry_1.values()), list(entry_2.values())],
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
                        'serviceCompanyId',
                        'companyMatchId',
                        'country',
                        'sector',
                        'type',
                        'service',
                        'source',
                        'sourceId',
                        'timestamp',
                    ],
                    'next': None,
                    'values': [list(entry_2.values())],
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
                        'serviceCompanyId',
                        'companyMatchId',
                        'country',
                        'sector',
                        'type',
                        'service',
                        'source',
                        'sourceId',
                        'timestamp',
                    ],
                    'next': None,
                    'values': [list(entry_1.values()), list(entry_2.values())],
                },
            ),
        )


def test_single_sector_filter(app):
    url = 'http://localhost:80/api/v1/get-company-countries-and-sectors-of-interest'
    with app.test_client() as app_context:
        utils.assert_api_response(
            app_context=app_context,
            api=url,
            params='sector=sector1',
            expected_response=(
                200,
                {
                    'headers': [
                        'serviceCompanyId',
                        'companyMatchId',
                        'country',
                        'sector',
                        'type',
                        'service',
                        'source',
                        'sourceId',
                        'timestamp',
                    ],
                    'next': None,
                    'values': [list(entry_1.values())],
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
                        'serviceCompanyId',
                        'companyMatchId',
                        'country',
                        'sector',
                        'type',
                        'service',
                        'source',
                        'sourceId',
                        'timestamp',
                    ],
                    'next': None,
                    'values': [list(entry_2.values())],
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
                        'serviceCompanyId',
                        'companyMatchId',
                        'country',
                        'sector',
                        'type',
                        'service',
                        'source',
                        'sourceId',
                        'timestamp',
                    ],
                    'next': None,
                    'values': [list(entry_1.values()), list(entry_2.values())],
                },
            ),
        )


def test_single_service_filter(app):
    url = 'http://localhost:80/api/v1/get-company-countries-and-sectors-of-interest'
    with app.test_client() as app_context:
        utils.assert_api_response(
            app_context=app_context,
            api=url,
            params='service=datahub',
            expected_response=(
                200,
                {
                    'headers': [
                        'serviceCompanyId',
                        'companyMatchId',
                        'country',
                        'sector',
                        'type',
                        'service',
                        'source',
                        'sourceId',
                        'timestamp',
                    ],
                    'next': None,
                    'values': [list(entry_1.values()), list(entry_2.values())],
                },
            ),
        )


def test_multiple_service_filter(app):
    url = 'http://localhost:80/api/v1/get-company-countries-and-sectors-of-interest'
    with app.test_client() as app_context:
        utils.assert_api_response(
            app_context=app_context,
            api=url,
            params='service=datahub&service=export-wins',
            expected_response=(
                200,
                {
                    'headers': [
                        'serviceCompanyId',
                        'companyMatchId',
                        'country',
                        'sector',
                        'type',
                        'service',
                        'source',
                        'sourceId',
                        'timestamp',
                    ],
                    'next': None,
                    'values': [
                        list(entry_1.values()),
                        list(entry_2.values()),
                        list(entry_3.values()),
                    ],
                },
            ),
        )


def test_single_match_id_filter(app):
    url = 'http://localhost:80/api/v1/get-company-countries-and-sectors-of-interest'
    with app.test_client() as app_context:
        utils.assert_api_response(
            app_context=app_context,
            api=url,
            params='company-match-id=1',
            expected_response=(
                200,
                {
                    'headers': [
                        'serviceCompanyId',
                        'companyMatchId',
                        'country',
                        'sector',
                        'type',
                        'service',
                        'source',
                        'sourceId',
                        'timestamp',
                    ],
                    'next': None,
                    'values': [list(entry_1.values())],
                },
            ),
        )


def test_multiple_match_id_filter(app):
    url = 'http://localhost:80/api/v1/get-company-countries-and-sectors-of-interest'
    with app.test_client() as app_context:
        utils.assert_api_response(
            app_context=app_context,
            api=url,
            params='company-match-id=1&company-match-id=2',
            expected_response=(
                200,
                {
                    'headers': [
                        'serviceCompanyId',
                        'companyMatchId',
                        'country',
                        'sector',
                        'type',
                        'service',
                        'source',
                        'sourceId',
                        'timestamp',
                    ],
                    'next': None,
                    'values': [list(entry_1.values()), list(entry_2.values())],
                },
            ),
        )


def test_single_type_filter(app):
    url = 'http://localhost:80/api/v1/get-company-countries-and-sectors-of-interest'
    with app.test_client() as app_context:
        utils.assert_api_response(
            app_context=app_context,
            api=url,
            params='type=exported',
            expected_response=(
                200,
                {
                    'headers': [
                        'serviceCompanyId',
                        'companyMatchId',
                        'country',
                        'sector',
                        'type',
                        'service',
                        'source',
                        'sourceId',
                        'timestamp',
                    ],
                    'next': None,
                    'values': [list(entry_2.values())],
                },
            ),
        )


def test_multiple_type_filter(app):
    url = 'http://localhost:80/api/v1/get-company-countries-and-sectors-of-interest'
    with app.test_client() as app_context:
        utils.assert_api_response(
            app_context=app_context,
            api=url,
            params='type=interested&type=exported',
            expected_response=(
                200,
                {
                    'headers': [
                        'serviceCompanyId',
                        'companyMatchId',
                        'country',
                        'sector',
                        'type',
                        'service',
                        'source',
                        'sourceId',
                        'timestamp',
                    ],
                    'next': None,
                    'values': [
                        list(entry_1.values()),
                        list(entry_2.values()),
                        list(entry_3.values()),
                    ],
                },
            ),
        )


def test_combined_filter(app):
    url = 'http://localhost:80/api/v1/get-company-countries-and-sectors-of-interest'
    with app.test_client() as app_context:
        utils.assert_api_response(
            app_context=app_context,
            api=url,
            params='type=interested&source=source1',
            expected_response=(
                200,
                {
                    'headers': [
                        'serviceCompanyId',
                        'companyMatchId',
                        'country',
                        'sector',
                        'type',
                        'service',
                        'source',
                        'sourceId',
                        'timestamp',
                    ],
                    'next': None,
                    'values': [list(entry_1.values())],
                },
            ),
        )


def test_pagination(app):
    url = 'http://localhost:80/api/v1/get-company-countries-and-sectors-of-interest'
    with app.test_client() as app_context:
        utils.assert_api_response(
            app_context=app_context,
            api=url,
            params='next-id=2',
            expected_response=(
                200,
                {
                    'headers': [
                        'serviceCompanyId',
                        'companyMatchId',
                        'country',
                        'sector',
                        'type',
                        'service',
                        'source',
                        'sourceId',
                        'timestamp',
                    ],
                    'next': None,
                    'values': [list(entry_2.values()), list(entry_3.values())],
                },
            ),
        )


def test_pagination_next(app):
    old_pagination_size = app.config['app']['pagination_size']
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
                        'serviceCompanyId',
                        'companyMatchId',
                        'country',
                        'sector',
                        'type',
                        'service',
                        'source',
                        'sourceId',
                        'timestamp',
                    ],
                    'next': 'http://localhost/api/v1/'
                    'get-company-countries-and-sectors-of-interest?'
                    'orientation=tabular&'
                    'next-id=2',
                    'values': [list(entry_1.values())],
                },
            ),
        )
    app.config['app']['pagination_size'] = old_pagination_size
