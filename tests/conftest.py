import datetime
import unittest

import pytest
import sqlalchemy_utils
from flask import make_response
from sqlalchemy.orm import close_all_sessions

from app.common import application
from app.common.db.models import HawkUsers
from app.common.views import ac, json_error

TESTING_DB_NAME_TEMPLATE = 'test_{}'
pytest_plugins = [
    "tests.fixtures.add_to_db",
]


@json_error
@ac.authentication_required
@ac.authorization_required
def mock_endpoint():
    return make_response('OK')


@pytest.fixture(scope='session')
def app():
    db_name = _create_testing_db_name()
    app = application.make_current_app_test_app(db_name)
    app.add_url_rule('/test/', 'test', mock_endpoint)
    ctx = app.app_context()
    ctx.push()
    yield app
    ctx.pop()


@pytest.fixture(scope='session')
def test_client(app):
    with app.test_client() as test_client:
        yield test_client


@pytest.fixture(scope='function')
def app_with_db(app):
    close_all_sessions()
    app.db.engine.dispose()
    sqlalchemy_utils.create_database(app.config['SQLALCHEMY_DATABASE_URI'])
    create_tables(app)
    yield app
    app.db.session.close()
    app.db.session.remove()
    sqlalchemy_utils.drop_database(app.config['SQLALCHEMY_DATABASE_URI'])


@pytest.fixture(scope='module')
def app_with_db_module(app):
    close_all_sessions()
    app.db.engine.dispose()
    sqlalchemy_utils.create_database(app.config['SQLALCHEMY_DATABASE_URI'])
    create_tables(app)
    yield app
    app.db.session.close()
    app.db.session.remove()
    sqlalchemy_utils.drop_database(app.config['SQLALCHEMY_DATABASE_URI'])


@pytest.fixture(scope='function')
def app_with_hawk_user(app_with_db):
    app_with_db.config['access_control'].update(
        {
            'hawk_enabled': True,
            'hawk_nonce_enabled': True,
            'hawk_algorithm': 'sha256',
            'hawk_accept_untrusted_content': True,
            'hawk_localtime_offset_in_seconds': 0,
            'hawk_timestamp_skew_in_seconds': 60,
        }
    )
    HawkUsers.add_user(
        client_id='iss1',
        client_key='secret1',
        client_scope=['*'],
        description='test authorization 1',
    )
    HawkUsers.add_user(
        client_id='iss2',
        client_key='secret2',
        client_scope=['invalid_scope'],
        description='test authorization 2',
    )
    HawkUsers.add_user(
        client_id='iss3',
        client_key='secret3',
        client_scope=['other_endpoint', 'mock_endpoint'],
        description='test authorization 3',
    )
    yield app_with_db


@pytest.fixture
def app_with_mock_cache(app):
    class CacheMock:
        cache = {}

        def set(self, key, value, ex):
            self.cache[key] = value

        def get(self, key):
            return self.cache.get(key, None)

    app_has_cache = hasattr(app, 'cache')
    if app_has_cache:
        original_cache = app
    app.cache = CacheMock()
    yield app
    if app_has_cache:
        app.cache = original_cache
    else:
        del app.cache


@pytest.fixture(scope="function")
def sso_authenticated_request():
    with unittest.mock.patch('app.common.sso.token.is_authenticated') as mock_is_authenticated:
        mock_is_authenticated.return_value = True
        yield


def _create_testing_db_name():
    time_str = _create_current_time_str()
    return TESTING_DB_NAME_TEMPLATE.format(time_str)


def _create_current_time_str():
    now = datetime.datetime.now()
    return now.strftime('%Y%m%d_%H%M%S_%f')


def create_tables(app):
    app.db.create_all()
