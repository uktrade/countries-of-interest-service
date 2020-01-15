from app.algorithm.interaction_coi_extraction import analyse_interactions


class Task:

    name = 'interactions_analysed_task'

    def __init__(self, *args, **kwargs):
        pass

    def __call__(self):
        return analyse_interactions()
