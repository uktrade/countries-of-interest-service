from flask import current_app as app

from flask_script import Manager

from app.algorithm.country_standardisation import standardize_countries as standardise
from app.algorithm.interaction_coi_extraction import analyse_interactions
from app.utils import log

AlgorithmCommand = Manager(app=app, usage='Development commands')


@AlgorithmCommand.command
@log('standardise countries to DIT reference dataset')
def standardise_countries():
    standardise()


@AlgorithmCommand.command
@log('extract coi from datahub interactions')
def interaction_coi_extraction():
    analyse_interactions()
