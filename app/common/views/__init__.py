from functools import wraps

import redis
from flask import current_app as flask_app, request
from flask import jsonify, make_response
from werkzeug.exceptions import BadRequest, NotFound, Unauthorized, UnprocessableEntity

from app.common.api.access_control import AccessControl
from app.common.db.models import HawkUsers

ac = AccessControl()


@ac.client_key_loader
def get_client_key(client_id):
    client_key = HawkUsers.get_client_key(client_id)
    if client_key:
        return client_key
    else:
        raise LookupError()


@ac.client_scope_loader
def get_client_scope(client_id):
    client_scope = HawkUsers.get_client_scope(client_id)
    if client_scope:
        return client_scope
    else:
        raise LookupError()


@ac.nonce_checker
def seen_nonce(sender_id, nonce, timestamp):
    key = f'{sender_id}:{nonce}:{timestamp}'
    try:
        if flask_app.cache.get(key):
            # We have already processed this nonce + timestamp.
            return True
        else:
            # Save this nonce + timestamp for later.
            flask_app.cache.set(key, 'True', ex=300)
            return False
    except redis.exceptions.ConnectionError as e:
        flask_app.logger.error(f'failed to connect to caching server: {str(e)}')
        return True


def json_error(f):
    @wraps(f)
    def error_handler(*args, **kwargs):
        try:
            response = f(*args, **kwargs)
        except NotFound:
            response = jsonify({})
            response.status_code = 404
        except BadRequest as e:
            response = jsonify({'error': e.description})
            response.status_code = 400
        except UnprocessableEntity as e:
            response = jsonify({'error': e.description})
            response.status_code = 422
        except Unauthorized:
            response = make_response('', 401)
        except Exception as e:
            flask_app.logger.error(f'unexpected exception for API request: {str(e)}')
            response = make_response('', 500)
        return response

    return error_handler


def response_orientation_decorator(view):
    @wraps(view)
    def wrapper(*args, **kwargs):
        orientation = request.args.get('orientation', 'tabular')
        return view(orientation, *args, **kwargs)

    return wrapper


def healthcheck():
    return jsonify({"status": "OK"})
