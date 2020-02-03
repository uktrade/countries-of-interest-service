from app.algorithm.interaction_coi_extraction import analyse_interactions


class Task:

    name = 'interactions_analysed'

    def __init__(self, *args, **kwargs):
        pass

    def __call__(self):
        analyse_interactions()
        return {
            'status': 200,
            'task': self.name,
        }
