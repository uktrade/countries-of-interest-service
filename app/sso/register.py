from urllib.parse import urljoin

from flask_security import Security, SQLAlchemyUserDatastore

from . import sso
from .models import Role, User
from .role import SSOClient as SSORoleBasedClient
from .token import SSOClient as SSOTokenClient


def register_sso_component(flask_app, role_based=True):
    # Setup Flask-Security
    if role_based:
        flask_app.config['SECURITY_PASSWORD_HASH'] = 'pbkdf2_sha512'
        flask_app.config['SECURITY_PASSWORD_SALT'] = 'salt'
        flask_app.config['SECURITY_TRACKABLE'] = True
        flask_app.config['SECURITY_USER_IDENTITY_ATTRIBUTES'] = 'user_id'
        flask_app.user_datastore = SQLAlchemyUserDatastore(flask_app.db, User, Role)
        flask_app.security = Security(flask_app, flask_app.user_datastore, register_blueprint=False)
        sso_client = SSORoleBasedClient
        user_datastore = flask_app.user_datastore
    else:
        sso_client = SSOTokenClient
        user_datastore = None

    # SSO
    sso_url = flask_app.config['sso']['base_url']
    flask_app.sso_client = sso_client(
        flask_app,
        sso_session_token_key=flask_app.config['session']['secret_key'],
        sso_url=sso_url,
        access_token_url=urljoin(sso_url, flask_app.config['sso']['access_token_path']),
        authorize_url=urljoin(sso_url, flask_app.config['sso']['authorize_path']),
        profile_url=urljoin(sso_url, flask_app.config['sso']['profile_path']),
        logout_url=urljoin(sso_url, flask_app.config['sso']['logout_path']),
        client_id=flask_app.config['sso']['client_id'],
        client_secret=flask_app.config['sso']['client_secret'],
        user_datastore=user_datastore,
    )
    try:
        flask_app.register_blueprint(sso)
    except AssertionError:
        pass
    return flask_app
