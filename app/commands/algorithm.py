from flask import current_app as app

from flask_script import Manager

from app.algorithm.country_standardisation import standardize_countries as standardise
from app.utils import log

AlgorithmCommand = Manager(app=app, usage='Development commands')


@AlgorithmCommand.command
@log('standardise countries to DIT reference dataset')
def standardise_countries():
    standardise()
