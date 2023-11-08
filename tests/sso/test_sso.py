import unittest.mock

import pytest
from authlib.integrations.flask_client import OAuthError
from flask import session

import app.sso as sso


@pytest.fixture(scope="session")
def sso_client(app):
    kwargs = {
        'access_token_url': 'access_token_url',
        'app': app,
        'authorize_url': 'authorize_url',
        'client_id': 'client_id',
        'client_secret': 'client_secret',
        'logout_url': 'logout_url',
        'profile_url': 'profile_url',
        'sso_session_token_key': 'sso_session_token_key',
        'sso_url': 'sso_url',
        'user_datastore': 'user_datastore',
    }

    client = sso.BaseSSOClient(**kwargs)
    client.process_login = lambda response: None
    client.process_logout = lambda: None

    return client


@pytest.fixture(scope="function")
def mock_oauth():
    with unittest.mock.patch('app.sso.OAuth') as mock_oauth:
        yield mock_oauth().register()


@pytest.fixture(scope="module")
def mock_url_for():
    with unittest.mock.patch('app.sso.url_for') as mock_url_for:
        yield mock_url_for


class TestInit:
    def test_sets_attributes(self, app, mock_oauth, sso_client):
        assert sso_client.sso_session_token_key == 'sso_session_token_key'
        assert sso_client.user_datastore == 'user_datastore'
        assert sso_client.db == app.db
        assert sso_client.profile_url == 'profile_url'
        assert sso_client.logout_url == 'logout_url'
        assert sso_client.oauth_broker == mock_oauth


class TestLogin:
    def test_returns_oauth_broker_authorize(self, app, mock_oauth, mock_url_for, sso_client):
        sso_client.login()
        mock_oauth.authorize_redirect.assert_called_once_with(
            callback=mock_url_for('sso.callback', _external=True)
        )


class TestLogout:
    def test_removes_session_key(self, app, sso_client):
        with app.test_request_context():
            session['sso_session_token_key'] = 'access_token'

        with app.test_client() as c:
            c.get('/logout')

        with app.test_request_context():
            assert len(session) == 0

    def test_redirect(self, app, sso_client):
        with app.test_request_context():
            response = sso_client.logout()
        assert response.status_code == 302
        assert response.location == 'logout_url'


class TestCallback:
    def test_no_access_token_provided(self, app, sso_client):
        with app.test_request_context('/?error=error&error_description=description'):
            with pytest.raises(OAuthError):
                sso_client.callback()
            assert 'sso_session_token_key' not in session

    def test_adds_token_to_session(self, app, mock_oauth, sso_client):
        response = 'access_token'
        mock_oauth.authorize_access_token.return_value = response
        sso_client.oauth_broker = mock_oauth
        with app.test_request_context():
            sso_client.callback()
            assert session['sso_session_token_key'] == 'access_token'


class TestGetProfile:
    def test_get_profile(self, app, mock_oauth, sso_client):

        mock_response = unittest.mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"name": "me"}
        mock_oauth.get.return_value = mock_response
        sso_client.oauth_broker = mock_oauth

        with app.test_request_context():
            response = sso_client.get_profile()

        assert response == {"name": "me"}


class TestGetNextUrl:
    def test_if_next_in_request(self, app, sso_client):
        with app.test_request_context('/?next=/apples'):
            response = sso_client._get_next_url()
        assert response == '/apples'

    def test_if_next_in_session(self, app, sso_client):
        with app.test_request_context():
            session['next'] = '/oranges'
            response = sso_client._get_next_url()
        assert response == '/oranges'

    def test_next_not_specified(self, app, sso_client):
        with app.test_request_context():
            response = sso_client._get_next_url()
        assert response == '/'

    def test_next_specified_in_request_and_session(self, app, sso_client):
        with app.test_request_context('/?next=/apples'):
            session['next'] = '/oranges'
            response = sso_client._get_next_url()
        assert response == '/apples'
