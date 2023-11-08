import os
import re
from logging.config import dictConfig

import certifi
import redis
from flask import Flask, json
from sqlalchemy.engine.url import make_url

from app.common import config
from app.common.api.settings import CustomJSONProvider
from app.common.commands.generic import cmd_group as generic_cmd
from app.common.views import healthcheck

logging_config = {
    'version': 1,
    'disable_existing_loggers': False,
    'root': {'level': 'INFO', 'handlers': ['console'], 'formatter': 'json'},
    'formatters': {
        'verbose': {'format': '[%(levelname)s] [%(name)s] %(message)s'},
        'json': {'()': 'app.common.api.settings.JSONLogFormatter'},
    },
    'handlers': {
        'console': {'level': 'DEBUG', 'class': 'logging.StreamHandler', 'formatter': 'json'}
    },
}

dictConfig(logging_config)


def get_or_create(config_overrides=()):
    from flask import current_app as flask_app

    if flask_app:
        return flask_app
    return _create_base_app(config_overrides)


def _create_base_app(config_overrides=()):
    flask_app = Flask(__name__)
    try:
        from app.application import config_location
    except ImportError:
        config_location = os.path.realpath(
            os.path.join(os.getcwd(), os.path.dirname(__file__), 'config')
        )

    flask_app.config.update(config.Config(config_location).all())

    try:
        from app.application import template_location
    except ImportError:
        template_location = os.path.realpath(
            os.path.join(os.getcwd(), os.path.dirname(__file__), 'templates')
        )
    flask_app.template_folder = template_location

    try:
        from app.application import static_location
    except ImportError:
        static_location = os.path.realpath(
            os.path.join(os.getcwd(), os.path.dirname(__file__), 'static')
        )
    flask_app.static_folder = static_location

    db_uri = re.sub(r'^postgres:', 'postgresql:', _load_uri_from_vcap_services('postgres') or '')
    if not db_uri:
        db_uri = flask_app.config['app']['database_url']

    flask_app.cli.add_command(generic_cmd)

    try:
        from app.commands import get_command_groups
    except ImportError:
        pass
    else:
        for cmd_group in get_command_groups():
            flask_app.cli.add_command(cmd_group)

    flask_app.config.update(
        {
            'TESTING': False,
            'SQLALCHEMY_DATABASE_URI': _create_sql_alchemy_connection_str(db_uri),
            # set SQLALCHEMY_TRACK_MODIFICATIONS to False because
            # default of None produces warnings, and track modifications
            # are not required
            'SQLALCHEMY_TRACK_MODIFICATIONS': False,
        }
    )

    flask_app.json = CustomJSONProvider(flask_app)
    flask_app.secret_key = flask_app.config['app']['secret_key']

    for config_override in config_overrides:
        flask_app.config = config_override(flask_app.config)

    return _register_components(flask_app)


def make_current_app_test_app(test_db_name):
    return get_or_create(
        (
            lambda config: {
                **config,
                'TESTING': True,
                'SQLALCHEMY_DATABASE_URI': _create_sql_alchemy_connection_str(
                    config['app']['database_url'], test_db_name
                ),
            },
        )
    )


def _register_components(flask_app):
    # Postgres DB
    from app.common.db.models import sql_alchemy
    from app.common.db.dbi import DBI

    sql_alchemy.init_app(flask_app)
    flask_app.db = sql_alchemy
    flask_app.dbi = DBI(sql_alchemy)

    # Routes
    from app.api import routes

    for rule, view_func in routes.RULES:
        flask_app.add_url_rule(rule, view_func=view_func)

    # Healthcheck
    flask_app.add_url_rule('/healthcheck/', view_func=healthcheck),

    # Cache
    redis_uri = _get_redis_url(flask_app)
    flask_app.cache = redis.from_url(redis_uri)

    return flask_app


def _get_redis_url(flask_app):
    redis_uri = _load_uri_from_vcap_services('redis')
    if not redis_uri:
        password = flask_app.config['cache'].get('password')
        redis_uri = (
            f"user:{password}"
            if password
            else "" f"{flask_app.config['cache']['host']}:" f"{flask_app.config['cache']['port']}"
        )
    if redis_uri.startswith('rediss://'):
        return f"{redis_uri}?ssl_ca_certs={certifi.where()}"
    return redis_uri


def _load_uri_from_vcap_services(service_type):
    if 'VCAP_SERVICES' in os.environ:
        vcap_services = os.environ.get('VCAP_SERVICES')
        services = json.loads(vcap_services)
        if service_type in services:
            services_of_type = services[service_type]
            for service in services_of_type:
                if 'credentials' in service:
                    if 'uri' in service['credentials']:
                        return service['credentials']['uri']
    return None


def _create_sql_alchemy_connection_str(cfg, db_name=None):
    url = make_url(cfg)
    if db_name:
        url = url.set(database=db_name)
    return url
