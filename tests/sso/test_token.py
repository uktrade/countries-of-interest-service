import unittest
import unittest.mock
from urllib.error import URLError

import pytest
from flask_oauthlib.client import OAuthException
from werkzeug.exceptions import HTTPException

import app.sso.token as token


@pytest.fixture
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

    return token.SSOClient(**kwargs)


class TestSSoClient:
    def test_process_login(self, sso_client):
        sso_response = unittest.mock.Mock()
        response = sso_client.process_login(sso_response)
        assert response is None

    def test_process_logout(self, sso_client):
        response = sso_client.process_logout()
        assert response is None


class TestLoginRequired:
    def test_if_authenticated_return_view(self, sso_authenticated_request):
        def view():
            return "view"

        wrapped_view = token.login_required(view)
        assert view() == wrapped_view()

    @unittest.mock.patch('app.sso.token.is_authenticated')
    @unittest.mock.patch('app.sso.token.redirect')
    @unittest.mock.patch('app.sso.token.request')
    @unittest.mock.patch('app.sso.token.url_for')
    def test_if_not_authenticated_redirect(self, url_for, request, redirect, is_authenticated):
        is_authenticated.return_value = False

        def view():
            return "view"

        wrapped_view = token.login_required(view)
        response = wrapped_view()
        redirect.assert_called_once()
        print('response:', response)


class TestIsAuthenticated:
    def test_if_get_profile_does_not_error_return_true(self, app):
        from flask import current_app as flask_app

        flask_app.sso_client = unittest.mock.Mock()
        response = token.is_authenticated()
        assert response is True

    def test_if_get_profile_error_is_caught_return_false(self, app):
        from flask import current_app as flask_app

        flask_app.sso_client = unittest.mock.Mock()
        handled_exceptions = [URLError('e'), OAuthException('e'), HTTPException('e')]
        flask_app.sso_client.get_profile.side_effect = handled_exceptions
        for e in handled_exceptions:
            response = token.is_authenticated()
            assert response is False

    def test_if_unhandled_exception_raise_exception(self, app):
        from flask import current_app as flask_app

        flask_app.sso_client = unittest.mock.Mock()
        flask_app.sso_client.get_profile.side_effect = Exception('unhandled')
        with pytest.raises(Exception):
            token.is_authenticated()
