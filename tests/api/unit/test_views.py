import datetime
import unittest.mock

import pytest
import redis
from data_engineering.common.views import get_client_scope, json_error, seen_nonce
from werkzeug.exceptions import BadRequest, NotFound


@pytest.fixture
def HawkUsers():
    path = 'data_engineering.common.db.models.HawkUsers'
    with unittest.mock.patch(path) as mock_obj:
        yield mock_obj


@pytest.fixture
def logging():
    path = 'data_engineering.common.views.flask_app.logger'
    with unittest.mock.patch(path) as mock_obj:
        yield mock_obj


class TestGetClientScope:
    def test_if_client_scope_is_not_found_return_error(self, app_with_db, HawkUsers):
        client_id = 'client_id'
        HawkUsers.get_client_scope.return_value = None
        with pytest.raises(LookupError):
            get_client_scope(client_id)


class TestSeenNonce:
    def test_if_there_is_a_redis_error_assume_nonce_has_been_seen(
        self, app_with_mock_cache, logging
    ):
        sender_id = 0
        nonce = 'asdf'
        timestamp = datetime.datetime(2019, 1, 1)
        app_with_mock_cache.cache.get = unittest.mock.Mock()
        app_with_mock_cache.cache.get.side_effect = redis.exceptions.ConnectionError()
        result = seen_nonce(sender_id, nonce, timestamp)
        assert result is True
        logging.error.assert_called_once()


class TestJsonError:
    def test_not_found(self, app):
        def f():
            raise NotFound()

        wrapped = json_error(f)
        response = wrapped()
        assert response.status_code == 404
        assert response.json == {}

    def test_bad_request(self, app):
        def f():
            raise BadRequest('error msg')

        wrapped = json_error(f)
        response = wrapped()
        assert response.status_code == 400
        assert response.json == {'error': 'error msg'}

    def test_generic_error(self, app):
        def f():
            raise Exception('error msg')

        wrapped = json_error(f)
        response = wrapped()
        assert response.status_code == 500
