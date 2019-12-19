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


def test_get_datahub_company_ids(app):
    url = 'http://localhost:80/api/v1/get-datahub-company-ids'
    with app.test_client() as app_context:
        utils.assert_api_response(
            app_context=app_context,
            api=url,
            expected_response=(
                200,
                {
                    'results': [
                        {'datahubCompanyId': '40e6215d-b5c6-4896-987c-f30f3678f608'}
                    ]
                },
            ),
        )
