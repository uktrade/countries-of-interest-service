from app.algorithm.interaction_coi_extraction.steps import process_interactions
from app.db.models.external import Interactions
from app.db.models.internal import InteractionsAnalysed
from app.utils import log

input_schema = 'public'
input_table = Interactions.__tablename__
output_schema = 'algorithm'
output_table = InteractionsAnalysed.__tablename__


def analyse_interactions():
    mapper = InteractionAnalyser()
    mapper.analyse()


class InteractionAnalyser:
    @log('extracting coi from interactions')
    def analyse(self):
        process_interactions(
            input_table, input_schema, output_schema, output_table, batch_size=1000
        )
