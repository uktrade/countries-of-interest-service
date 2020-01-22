from flask.cli import AppGroup

from app.algorithm.country_standardisation import standardise_countries as standardise
from app.algorithm.interaction_coi_extraction import analyse_interactions
from app.utils import log

cmd_group = AppGroup('algorithm', help='Commands to run algorithm')


@cmd_group.command('standardise_countries')
@log.write('standardise countries to DIT reference dataset')
def standardise_countries():
    standardise()


@cmd_group.command('interaction_coi_extraction')
@log.write('extract coi from datahub interactions')
def interaction_coi_extraction():
    analyse_interactions()
