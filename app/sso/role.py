from functools import wraps

from flask import redirect, request, url_for
from flask_security import current_user, login_user, logout_user

from . import BaseSSOClient


class SSOClient(BaseSSOClient):
    def process_login(self, resp):
        # Retrieve user profile from broker
        profile = self.get_profile()

        # Add/Update user in local DB
        user = self.user_datastore.get_user(profile['user_id'])
        if not user:
            self.user_datastore.create_user(
                first_name=profile['first_name'],
                last_name=profile['last_name'],
                email=profile['email'],
                user_id=profile['user_id'],
                roles=[],
            )
            user = self.user_datastore.get_user(profile['user_id'])
        login_user(user)
        self.user_datastore.commit()

    def process_logout(self):
        logout_user()


def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not is_authenticated():
            return redirect(url_for('sso.login', next=request.url))

        return f(*args, **kwargs)

    return decorated_function


def is_authenticated():
    """Is the current user authenticated"""
    if current_user.is_authenticated:
        return True
    return False
