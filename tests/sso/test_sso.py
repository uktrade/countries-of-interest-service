from flask import make_response, redirect, url_for, request

from mohawk import Sender
from mohawk.util import utc_now

import os
import pytest

from app.api.views import api, json_error, jsonify
from app.sso.token import login_required


import os
import logging
from unittest import mock


logging.basicConfig()
logger = logging.getLogger(__name__)

# Change logging level here.
logger.setLevel(os.environ.get('LOG_LEVEL', logging.DEBUG))


@api.route('/test_sso/', methods=["GET"])
@json_error
@login_required
def mock_sso_endpoint():
    return make_response('SSO Login OK')


@api.route('/o/authorize/', methods=["GET"])
def mock_auth_endpoint():
    return redirect(url_for('sso.callback', code='CODE'))


@api.route('/o/token/', methods=["POST", "GET"])
@json_error
def mock_token_endpoint():
    return jsonify({})


@api.route('/api/v1/user/me/', methods=["GET", "POST"])
def mock_profile_endpoint():
    return jsonify({'user_id': 1})


@api.route('/hello/', methods=["GET", "POST"])
def mock_hello_endpoint():
    return jsonify({})


class CacheMock:
    cache = {}

    def set(self, key, value, ex):
        self.cache[key] = value

    def get(self, key):
        return self.cache.get(key, None)


class TestAuthentication:
    @pytest.fixture(autouse=True)
    def setup(self, app_with_db):
        os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
        app_with_db.cache = CacheMock()
        self.app = app_with_db
        self.app.config.update({'OAUTHLIB_INSECURE_TRANSPORT': '1'})
        self.client_id = 'iss1'
        self.client_key = 'secret1'
        self.client_scope = ['*']
        self.description = 'test authentication'

    def get_client(self):
        with self.app.test_client() as client:
            with client.session_transaction() as session:
                session['access_token'] = 'ENTER'
                session['_authbroker_token'] = 'ENTER'
            return client

    def test_successful_authentication(self):
        c = self.get_client()
        with mock.patch(
            'flask_oauthlib.client.OAuthRemoteApp.authorized_response'
        ) as auth_response:
            auth_response.return_value = {'access_token': 'ENTER'}

            response = c.get('/test_sso/', follow_redirects=True)
            assert auth_response.called
            assert response.data == 200