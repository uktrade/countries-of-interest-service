import os
from logging.config import dictConfig

import certifi
import redis
from celery import Celery
from flask import Flask, json
from sqlalchemy.engine.url import make_url

from app import config
from app.commands.algorithm import cmd_group as algorithm_cmd
from app.commands.csv import cmd_group as csv_cmd
from app.commands.database import cmd_group as database_cmd
from app.commands.dev import cmd_group as dev_cmd
from app.sso.register import register_sso_component


logging_config = {
    'version': 1,
    'disable_existing_loggers': False,
    'root': {'level': 'INFO', 'handlers': ['console'], 'formatter': 'json'},
    'formatters': {
        'verbose': {'format': '[%(levelname)s] [%(name)s] %(message)s'},
        'json': {'()': 'app.api.settings.JSONLogFormatter'},
    },
    'handlers': {
        'console': {'level': 'DEBUG', 'class': 'logging.StreamHandler', 'formatter': 'json'}
    },
}

dictConfig(logging_config)


def make_celery(flask_app):
    backend_url = _get_redis_url(flask_app) or ""
    flask_app.config['broker_url'] = f"{backend_url}/0"
    flask_app.config['result_backend'] = f"{backend_url}/0"

    celery = Celery('application', broker=backend_url)

    celery.conf.broker_transport_options = {
        'max_retries': 3,
        'interval_start': 0,
        'interval_step': 0.2,
        'interval_max': 0.2,
    }
    return celery


def get_or_create():
    from flask import current_app as flask_app

    if not flask_app:
        flask_app = _create_base_app()
    return flask_app


def make_current_app_test_app(test_db_name):
    flask_app = get_or_create()
    postgres_db_config = (
        os.environ.get('DATABASE_URL')
        if 'DATABASE_URL' in os.environ
        else config.get_config()['app']['database_url']
    )
    flask_app.config.update(
        {
            'TESTING': True,
            'SQLALCHEMY_DATABASE_URI': _create_sql_alchemy_connection_str(
                postgres_db_config, test_db_name
            ),
        }
    )
    return flask_app


def _create_base_app():
    flask_app = Flask(__name__)
    flask_app.config.update(config.get_config())
    flask_app.cli.add_command(dev_cmd)
    flask_app.cli.add_command(algorithm_cmd)
    flask_app.cli.add_command(database_cmd)
    flask_app.cli.add_command(csv_cmd)

    postgres_db_config = (
        os.environ.get('DATABASE_URL')
        if 'DATABASE_URL' in os.environ
        else config.get_config()['app']['database_url']
    )
    flask_app.config.update(
        {
            'TESTING': False,
            'SQLALCHEMY_DATABASE_URI': _create_sql_alchemy_connection_str(postgres_db_config),
            # set SQLALCHEMY_TRACK_MODIFICATIONS to False because
            # default of None produces warnings, and track modifications
            # are not required
            'SQLALCHEMY_TRACK_MODIFICATIONS': False,
        }
    )

    flask_app.secret_key = flask_app.config['app']['secret_key']
    from app.api.settings import CustomJSONEncoder

    flask_app.json_encoder = CustomJSONEncoder
    celery = make_celery(flask_app)
    flask_app.celery = celery
    flask_app = _register_components(flask_app)
    flask_app = register_sso_component(flask_app, role_based=False)
    return flask_app


def _register_components(flask_app):
    from app.api.views import api

    # Postgres DB
    from app.db import sql_alchemy

    sql_alchemy.session = sql_alchemy.create_scoped_session()
    sql_alchemy.init_app(flask_app)
    flask_app.db = sql_alchemy

    # API
    flask_app.register_blueprint(api)

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
        url.database = db_name
    return url
