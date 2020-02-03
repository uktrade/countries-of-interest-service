from app.algorithm.interaction_coi_extraction import analyse_interactions
from app.config import constants


class Task:

    name = constants.Task.INTERACTIONS_ANALYSED.value

    def __init__(self, *args, **kwargs):
        pass

    def __call__(self):
        analyse_interactions()
        return {
            'status': 200,
            'task': self.name,
        }
