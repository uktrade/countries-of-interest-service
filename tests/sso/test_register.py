import unittest
import unittest.mock

import pytest

from app.sso.register import register_sso_component

@pytest.fixture
def mock_app():
        app = unittest.mock.Mock()
        app.config = {
            'sso': {
                'base_url': 'base_url',
                'access_token_path': 'acces_token_path',
                'authorize_path': 'authorize_path',
                'profile_path': 'profile_path',
                'logout_path': 'logout_path',
                'client_id': 'client_id',
                'client_secret': 'client_secret',
            },
            'session': {'secret_key': 'shhh'},
        }
        return app

@pytest.fixture
def mock_role_based_client():
    with unittest.mock.patch('app.sso.register.SSORoleBasedClient') as mock:
        yield mock

@pytest.fixture
def mock_token_client():
    with unittest.mock.patch('app.sso.register.SSOTokenClient') as mock:
        yield mock

@pytest.fixture
def mock_security():
    with unittest.mock.patch('app.sso.register.Security') as mock_security:
        yield mock_security

@pytest.fixture
def mock_sql_alchemy():
    with unittest.mock.patch('app.sso.register.SQLAlchemyUserDatastore') as mock:
        yield mock_mock

class TestRegisterSsoComponent:

    def test_set_token_sso_client_if_not_role_based(self, mock_app, mock_token_client):
        role_based = False
        output = register_sso_component(mock_app, role_based)
        assert mock_app.sso_client == mock_token_client.return_value

    def test_set_role_based_sso_client_if_role_based(
            self,
            mock_app,
            mock_role_based_client,
            mock_security
    ):
        role_based = True
        output = register_sso_component(mock_app, role_based)
        assert mock_app.sso_client == mock_role_based_client.return_value




        

        
