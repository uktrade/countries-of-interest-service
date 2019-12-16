import certifi

from flask import Blueprint, redirect, request, session, url_for

from flask_oauthlib.client import OAuth

from flask_security import login_user, logout_user

import werkzeug
from werkzeug.exceptions import abort

from app.db.models import Role

sso = Blueprint('sso', __name__)


class SSOClient:
    def __init__(
        self,
        app,
        sso_session_token_key,
        sso_url,
        access_token_url,
        authorize_url,
        profile_url,
        logout_url,
        client_id,
        client_secret,
    ):
        self.sso_session_token_key = sso_session_token_key
        self.user_datastore = app.user_datastore
        self.db = app.db
        self.profile_url = profile_url
        self.logout_url = logout_url

        # add routes to blueprint

        # initialize oauth broker
        oauth = OAuth(app)
        oauth.ca_certs = certifi.where()
        oauth_broker = oauth.remote_app(
            'authbroker',
            base_url=sso_url,
            request_token_url=None,
            access_token_url=access_token_url,
            authorize_url=authorize_url,
            consumer_key=client_id,
            consumer_secret=client_secret,
            access_token_method='POST',
            request_token_params={'state': lambda: werkzeug.security.gen_salt(10)},
        )

        oauth_broker.tokengetter(lambda: session.get(self.sso_session_token_key))
        self.oauth_broker = oauth_broker

    def login(self):
        return self.oauth_broker.authorize(
            callback=url_for('sso.callback', _external=True)
        )

    def logout(self):
        session.pop(self.sso_session_token_key, None)
        logout_user()
        return redirect(self.logout_url)

    def callback(self):
        resp = self.oauth_broker.authorized_response()

        if resp is None or resp.get('access_token') is None:
            return 'Access denied: reason=%s error=%s resp=%s' % (
                request.args['error'],
                request.args['error_description'],
                resp,
            )
        else:
            session[self.sso_session_token_key] = (resp['access_token'], '')

            # Retrieve user profile from broker
            profile = self.get_profile()

            # Add/Update user in local DB
            user = self.user_datastore.get_user(profile['user_id'])
            if not user:
                user_role = Role(name='user')
                self.user_datastore.create_user(
                    first_name=profile['first_name'],
                    last_name=profile['last_name'],
                    email=profile['email'],
                    user_id=profile['user_id'],
                    roles=[user_role],
                )
                user = self.user_datastore.get_user(profile['user_id'])
            login_user(user)
            self.user_datastore.commit()
            return redirect(self._get_next_url())

    def get_profile(self):
        response = self.oauth_broker.get(self.profile_url)

        if response.status != 200:
            abort(403)

        return response.data

    def _get_next_url(self):
        if 'next' in request.args:
            next_url = request.args['next']
        elif 'next' in session:
            next_url = session.pop('next', None)
        else:
            next_url = None
        return next_url
