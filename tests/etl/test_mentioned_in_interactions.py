import datetime

from flask import current_app

from app.db.models.internal import MentionedInInteractions
from app.etl.tasks.mentioned_in_interactions import Task


class TestTask:
    def test(
        self, add_datahub_interaction, add_interactions_analysed,
    ):

        add_datahub_interaction(
            [
                {
                    'id': 0,
                    'datahub_interaction_id': '48599843-2d10-490b-854d-7d97f16f0d13',
                    'datahub_company_id': 'eb75deae-1ce8-4cff-a0e9-645330614db6',
                    'subject': 'subject_0',
                    'notes': 'notes_0',
                    'created_on': '2019-01-01',
                },
                {
                    'id': 1,
                    'datahub_interaction_id': '592eb397-4211-4c8b-a636-61215e9bdab0',
                    'datahub_company_id': '698c6beb-f4e8-4949-8bfd-4b49b447937e',
                    'subject': 'subject_1',
                    'notes': 'notes_1',
                    'created_on': '2019-01-02',
                },
                {
                    'id': 2,
                    'datahub_interaction_id': '7c7636c5-d50e-4bb5-9b1c-195754b048a1',
                    'datahub_company_id': 'cc97941e-8cf8-4949-a880-b0b08f6265ff',
                    'subject': 'subject_2',
                    'notes': 'notes_2',
                    'created_on': '2019-01-03',
                },
            ]
        )

        add_interactions_analysed(
            [
                {
                    'id': 0,
                    'place': 'Milan',
                    'standardized_place': 'Italy',
                    'action': 'action_0',
                    'type': 'type_0',
                    'context': ['context_0'],
                    'negation': False,
                },
                {
                    'id': 2,
                    'place': 'Moldova',
                    'standardized_place': 'Moldova',
                    'action': 'action_2',
                    'type': 'type_2',
                    'context': ['context_2'],
                    'negation': True,
                },
            ]
        )

        task = Task()
        task()

        session = current_app.db.session
        mentioned_in_interactions = session.query(MentionedInInteractions).all()

        assert len(mentioned_in_interactions) == 2
        mention_0, mention_1 = mentioned_in_interactions

        assert mention_0.company_id == 'eb75deae-1ce8-4cff-a0e9-645330614db6'
        assert mention_0.country_of_interest == 'Italy'
        assert mention_0.interaction_id == '48599843-2d10-490b-854d-7d97f16f0d13'
        assert mention_0.timestamp == datetime.datetime(2019, 1, 1)

        assert mention_1.company_id == 'cc97941e-8cf8-4949-a880-b0b08f6265ff'
        assert mention_1.country_of_interest == 'Moldova'
        assert mention_1.interaction_id == '7c7636c5-d50e-4bb5-9b1c-195754b048a1'
        assert mention_1.timestamp == datetime.datetime(2019, 1, 3)
