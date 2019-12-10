import os

from authbroker_client import authbroker_blueprint

from celery import Celery

from flask import Flask, json

from app import config
from app.api.settings import CustomJSONEncoder
from app.api.views import api


def make_celery(app):
    app.config['CELERY_BROKER_URL'] = 'redis://redis:6379/0'
    app.config['result_backend'] = 'redis://redis:6379/0'
    # create context tasks in celery
    celery = Celery('application', broker=app.config['CELERY_BROKER_URL'])

    celery.conf.broker_transport_options = {
        'max_retries': 3,
        'interval_start': 0,
        'interval_step': 0.2,
        'interval_max': 0.2,
    }
    celery.conf.result_backend_transport_options = {'visibility_timeout': 18000}
    TaskBase = celery.Task

    class ContextTask(TaskBase):
        abstract = True
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return TaskBase.__call__(self, *args, **kwargs)
    celery.Task = ContextTask
    return celery


def get_or_create():
    from flask import current_app as flask_app

    if not flask_app:
        flask_app = _create_base_app()
    return flask_app


def _create_base_app():
    flask_app = Flask(__name__)
    flask_app.config.update(config.get_config())
    flask_app.config['ABC_BASE_URL'] = flask_app.config['sso']['host']
    flask_app.config['ABC_CLIENT_ID'] = flask_app.config['sso']['abc_client_id']
    flask_app.config['ABC_CLIENT_SECRET'] = flask_app.config['sso']['abc_client_secret']
    flask_app.secret_key = flask_app.config['app']['secret_key']
    flask_app.json_encoder = CustomJSONEncoder
    celery = make_celery(flask_app)
    flask_app.celery = celery
    flask_app = _register_components(flask_app)
    return flask_app


def _register_components(flask_app):
    flask_app.register_blueprint(api)
    flask_app.register_blueprint(authbroker_blueprint)
    return flask_app


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


celery_app = get_or_create().celery
