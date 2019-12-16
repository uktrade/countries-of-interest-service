import pytest

import tests.api.utils as utils


@pytest.fixture(scope='module', autouse=True)
def setup_function(app, add_companies_house_company_numbers):
    app.config['access_control']['hawk_enabled'] = False
    add_companies_house_company_numbers(
        [
            {
                'datahub_company_id': '40e6215d-b5c6-4896-987c-f30f3678f608',
                'companies_house_company_number': 'ch1',
            }
        ]
    )


def test_get_companies_house_company_numbers(app):
    with app.test_client() as app_context:
        utils.assert_api_response(
            app_context=app_context,
            api='http://localhost:80/api/v1/get-companies-house-company-numbers',
            expected_response=(
                200,
                {'results': [{'companies_house_company_number': 'ch1'}]},
            ),
        )
