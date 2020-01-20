from app.algorithm.interaction_coi_extraction.steps import process_interactions
from app.db.models.external import Interactions
from app.db.models.internal import (
    InteractionsAnalysed,
    InteractionsAnalysedInteractionIdLog,
)
from app.utils import log

input_schema = Interactions.get_schema()
input_table = Interactions.__tablename__
log_table = InteractionsAnalysedInteractionIdLog.__tablename__
output_schema = InteractionsAnalysed.get_schema()
output_table = InteractionsAnalysed.__tablename__


def analyse_interactions():
    mapper = InteractionAnalyser()
    mapper.analyse()


class InteractionAnalyser:
    @log('extracting coi from interactions')
    def analyse(self):
        process_interactions(
            input_table,
            input_schema,
            log_table,
            output_schema,
            output_table,
            batch_size=1000,
        )
