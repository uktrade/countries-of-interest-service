import datetime
import unittest.mock

import redis
from data_engineering.common.views import get_client_scope, json_error, seen_nonce
from werkzeug.exceptions import BadRequest, NotFound


class TestGetClientScope(unittest.TestCase):
    @unittest.mock.patch('data_engineering.common.db.models.HawkUsers')
    def test_if_client_scope_is_not_found_return_error(self, HawkUsers):
        client_id = 0
        HawkUsers.get_client_scope.return_value = None
        with self.assertRaises(LookupError):
            get_client_scope(client_id)


class TestSeenNonce(unittest.TestCase):
    @unittest.mock.patch('app.api.views.flask_app.logger')
    @unittest.mock.patch('app.api.views.flask_app')
    def test_if_there_is_a_redis_error_assume_nonce_has_been_seen(self, app, logging):
        sender_id = 0
        nonce = 'asdf'
        timestamp = datetime.datetime(2019, 1, 1)
        app.cache.get.side_effect = redis.exceptions.ConnectionError()
        result = seen_nonce(sender_id, nonce, timestamp)
        self.assertTrue(result)
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
