from functools import wraps
from urllib.error import URLError

from authlib.integrations.flask_client import OAuthError
from flask import redirect, request, url_for
from werkzeug.exceptions import HTTPException

from . import BaseSSOClient


class SSOClient(BaseSSOClient):
    def process_login(self, resp):
        pass

    def process_logout(self):
        pass


def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not is_authenticated():
            return redirect(url_for('sso.login', next=request.url))

        return f(*args, **kwargs)

    return decorated_function


def is_authenticated():
    from flask import current_app as flask_app

    try:
        flask_app.sso_client.get_profile()
    except (URLError, OAuthError, HTTPException):
        return False
    return True
