import certifi
from authlib.integrations.flask_client import OAuth
from flask import Blueprint, redirect, request, session, url_for
from werkzeug.exceptions import abort


sso = Blueprint('sso', __name__)


class BaseSSOClient:
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
        user_datastore,
    ):
        self.sso_session_token_key = sso_session_token_key
        self.user_datastore = user_datastore
        self.db = app.db
        self.profile_url = profile_url
        self.logout_url = logout_url

        # add routes to blueprint
        sso.add_url_rule('/login', view_func=self.login, methods=['GET'])
        sso.add_url_rule('/logout', view_func=self.logout, methods=['GET'])
        sso.add_url_rule('/login/authorised', view_func=self.callback, methods=['GET'])

        # initialize oauth broker
        oauth = OAuth(app)
        oauth.ca_certs = certifi.where()
        oauth_broker = oauth.register(
            'authbroker',
            api_base_url=sso_url,
            access_token_url=access_token_url,
            authorize_url=authorize_url,
            client_id=client_id,
            client_secret=client_secret,
            access_token_method='POST',
            fetch_token=lambda: session.get(self.sso_session_token_key),
        )

        self.oauth_broker = oauth_broker

    def login(self):
        return self.oauth_broker.authorize_redirect(
            callback=url_for('sso.callback', _external=True)
        )

    def logout(self):
        session.pop(self.sso_session_token_key, None)
        self.process_logout()
        return redirect(self.logout_url)

    def callback(self):
        access_token = self.oauth_broker.authorize_access_token()

        if access_token is None:
            return 'Access denied: reason=%s error=%s' % (
                request.args['error'],
                request.args['error_description'],
            )
        else:
            session[self.sso_session_token_key] = access_token
            self.process_login(access_token)
            return redirect(self._get_next_url())

    def process_login(self, access_token):
        raise NotImplementedError('Process login required')

    def process_logout(self):
        raise NotImplementedError('Process logout required')

    def get_profile(self):
        response = self.oauth_broker.get(self.profile_url)

        if response.status_code != 200:
            abort(403)

        return response.json()

    def _get_next_url(self):
        if 'next' in request.args:
            next_url = request.args['next']
        elif 'next' in session:
            next_url = session.pop('next', None)
        else:
            next_url = '/'
        return next_url
