import datetime

import pytest
from flask import current_app as flask_app
from spacy.language import Language

import app.algorithm.interaction_coi_extraction as mapper
from app.common.tests.utils import rows_equal_query_results
from app.db.models.external import Interactions
from app.db.models.internal import InteractionsAnalysedInteractionIdLog


@pytest.fixture
def add_objects(app_with_db):
    def _method(Model, records):
        record_objects = []
        for record in records:
            record_objects.append(Model.get_or_create(**record))
        return record_objects

    return _method


@pytest.fixture
def mock_datetime(mocker):
    return mocker.patch.object(mapper.steps, 'datetime')


@pytest.fixture
def add_side_effect(mocker):
    def _method(outputs):
        outputs = outputs[::-1]  # reverse the list

        def _side_effect():
            return Language()

        mocker.patch.object(mapper.steps, '_load_model', side_effect=_side_effect)

        def _side_effect(interaction_doc):
            return outputs.pop()

        mocker.patch.object(mapper.steps, '_analyse_interaction', side_effect=_side_effect)

    return _method


def test_interaction_coi_extraction_when_interaction_does_not_have_a_note(
    add_objects, add_side_effect, mock_datetime
):
    # Test mapping
    add_objects(
        Interactions,
        [
            {
                'datahub_interaction_id': '18c3e449-ce56-45b4-93d3-362960622ca2',
                'datahub_company_id': 'df14625d-c89b-4efd-bc5a-9b46f68b71bb',
                'notes': None,
            }
        ],
    )

    output = [()]
    add_side_effect(output)

    mock_datetime.datetime.now.return_value = datetime.datetime(2020, 1, 1)

    mapper.analyse_interactions()

    expected_rows = []
    assert rows_equal_query_results(
        flask_app.dbi,
        expected_rows,
        f'SELECT * FROM "{mapper.output_schema}"."{mapper.output_table}"',
    )

    session = flask_app.db.session
    interaction_ids = session.query(InteractionsAnalysedInteractionIdLog).all()
    assert len(interaction_ids) == 1
    assert str(interaction_ids[0].datahub_interaction_id) == '18c3e449-ce56-45b4-93d3-362960622ca2'
    assert interaction_ids[0].analysed_at == datetime.datetime(2020, 1, 1)


def test_interaction_coi_extraction(add_objects, add_side_effect, mock_datetime):
    # Test mapping
    add_objects(
        Interactions,
        [
            {
                'datahub_interaction_id': '52552367-436f-4a5d-84a2-dbf4ffeddb76',
                'datahub_company_id': 'df14625d-c89b-4efd-bc5a-9b46f68b71bb',
                'notes': 'Google exports to Brussels',
            }
        ],
    )

    output = [[('Brussels', 'Belgium', 'exported', 'GPE', ['export'], False)]]
    add_side_effect(output)

    mock_datetime.datetime.now.return_value = datetime.datetime(2020, 1, 1)

    mapper.analyse_interactions()

    expected_rows = [
        (
            1,
            '52552367-436f-4a5d-84a2-dbf4ffeddb76',
            'Brussels',
            'Belgium',
            'exported',
            'GPE',
            ['export'],
            False,
        )
    ]
    assert rows_equal_query_results(
        flask_app.dbi,
        expected_rows,
        f'SELECT * FROM "{mapper.output_schema}"."{mapper.output_table}"',
    )

    session = flask_app.db.session
    interaction_ids = session.query(InteractionsAnalysedInteractionIdLog).all()
    assert len(interaction_ids) == 1
    assert str(interaction_ids[0].datahub_interaction_id) == '52552367-436f-4a5d-84a2-dbf4ffeddb76'
    assert interaction_ids[0].analysed_at == datetime.datetime(2020, 1, 1)


def test_interaction_already_seen(add_objects, add_side_effect):
    add_objects(
        Interactions,
        [
            {
                'id': 1,
                'datahub_interaction_id': '52552367-436f-4a5d-84a2-dbf4ffeddb76',
                'datahub_company_id': 'df14625d-c89b-4efd-bc5a-9b46f68b71bb',
                'notes': 'Google exports to Brussels',
            },
            {
                'id': 2,
                'datahub_interaction_id': '52552367-436f-4a5d-84a2-dbf4ffeddb77',
                'datahub_company_id': 'df14625d-c89b-4efd-bc5a-9b46f68b71bb',
                'notes': 'nothing mentioning a location',
            },
            {
                'id': 3,
                'datahub_interaction_id': '52552367-436f-4a5d-84a2-dbf4ffeddb78',
                'datahub_company_id': 'df14625d-c89b-4efd-bc5a-9b46f68b71bb',
                'notes': 'China',
            },
        ],
    )

    add_objects(
        InteractionsAnalysedInteractionIdLog,
        [{'datahub_interaction_id': '52552367-436f-4a5d-84a2-dbf4ffeddb76'}],
    )

    outputs = [
        [],  # no country information
        [('China', 'China', 'exported', 'GPE', ['export'], False)],
    ]
    add_side_effect(outputs)

    mapper.analyse_interactions()

    expected_rows = [
        (
            '52552367-436f-4a5d-84a2-dbf4ffeddb78',
            'China',
            'China',
            'exported',
            'GPE',
            ['export'],
            False,
        ),
    ]

    assert rows_equal_query_results(
        flask_app.dbi,
        expected_rows,
        f'SELECT datahub_interaction_id,place,'
        f'standardized_place,action,type,context,negation'
        f' FROM "{mapper.output_schema}"."{mapper.output_table}"',
    )

    session = flask_app.db.session
    interaction_ids = session.query(InteractionsAnalysedInteractionIdLog).all()
    assert len(interaction_ids) == 3
