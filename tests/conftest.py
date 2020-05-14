import unittest.mock

import pytest

pytest_plugins = [
    "tests.fixtures.add_to_db",
    "data_engineering.common.tests.conftest",
]


@pytest.fixture(scope="function")
def sso_authenticated_request():
    with unittest.mock.patch('app.sso.token.is_authenticated') as mock_is_authenticated:
        mock_is_authenticated.return_value = True
        yield
