import datetime
import unittest.mock

import pytest
import sqlalchemy_utils

from app import application
from app.db.db_utils import create_schemas


pytest_plugins = [
    "tests.fixtures.add_to_db",
]

TESTING_DB_NAME_TEMPLATE = 'coi_test_{}'


@pytest.fixture(scope='session')
def app():
    db_name = _create_testing_db_name()
    app = application.make_current_app_test_app(db_name)
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
    app.db.session.close_all()
    app.db.engine.dispose()
    sqlalchemy_utils.create_database(app.config['SQLALCHEMY_DATABASE_URI'],)
    create_tables(app)
    yield app
    app.db.session.close()
    app.db.session.remove()
    app.db.drop_all()
    sqlalchemy_utils.drop_database(app.config['SQLALCHEMY_DATABASE_URI'])


@pytest.fixture(scope='module')
def app_with_db_module(app):
    app.db.session.close_all()
    app.db.engine.dispose()
    sqlalchemy_utils.create_database(app.config['SQLALCHEMY_DATABASE_URI'],)
    create_tables(app)
    yield app
    app.db.session.close()
    app.db.session.remove()
    app.db.drop_all()
    sqlalchemy_utils.drop_database(app.config['SQLALCHEMY_DATABASE_URI'])


@pytest.fixture(scope="function")
def sso_authenticated_request():
    with unittest.mock.patch('app.sso.token.is_authenticated') as mock_is_authenticated:
        mock_is_authenticated.return_value = True
        yield


def _create_testing_db_name():
    time_str = _create_current_time_str()
    return TESTING_DB_NAME_TEMPLATE.format(time_str)


def _create_current_time_str():
    now = datetime.datetime.now()
    return now.strftime('%Y%m%d_%H%M%S_%f')


def create_tables(app):
    create_schemas(app.db.engine)
    app.db.create_all()
