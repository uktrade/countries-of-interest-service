from app.algorithm.interaction_coi_extraction import analyse_interactions
from app.db.models.internal import InteractionsAnalysed


class Task:

    name = 'interactions_analysed_task'

    def __init__(self, *args, **kwargs):
        pass

    def __call__(self):
        analyse_interactions()
        return {
            'status': 200,
            'n_rows': None,
            'table': InteractionsAnalysed.__tablename__,
        }
