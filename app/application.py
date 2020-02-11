import os
import re
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


def get_or_create():
    from flask import current_app as flask_app

    if not flask_app:
        flask_app = _create_base_app()
    return flask_app


def _create_base_app():
    flask_app = Flask(__name__)
    flask_app.config.update(config.get_config())
    flask_app.cli.add_command(dev_cmd)
    flask_app.cli.add_command(algorithm_cmd)
    flask_app.cli.add_command(database_cmd)
    flask_app.cli.add_command(csv_cmd)

    dsn = os.environ['DATABASE_DSN__datasets_1']
    parsed = dict(re.findall(r'([a-z]+)=([\S]+)', dsn))
    db_uri = f"postgresql://{parsed['user']}:{parsed['password']}@{parsed['host']}:{parsed['port']}/{parsed['dbname']}"
    flask_app.config.update(
        {
            'TESTING': False,
            'SQLALCHEMY_DATABASE_URI': db_uri,
            # set SQLALCHEMY_TRACK_MODIFICATIONS to False because
            # default of None produces warnings, and track modifications
            # are not required
            'SQLALCHEMY_TRACK_MODIFICATIONS': False,
        }
    )

    flask_app.secret_key = flask_app.config['app']['secret_key']
    from app.api.settings import CustomJSONEncoder

    flask_app.json_encoder = CustomJSONEncoder
    flask_app = _register_components(flask_app)
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
    return flask_app
