import pytest

from spacy.language import Language

import app.algorithm.interaction_coi_extraction as mapper

from tests.utils import rows_equal_query_results


@pytest.fixture(scope='function', autouse=True)
def setup_function(mocker):
    def _side_effect():
        return Language()

    mocker.patch.object(mapper.steps, '_load_model', side_effect=_side_effect)

    def _side_effect(interaction_doc):
        return [('Brussels', 'Belgium', 'exported', 'GPE', ['export'], False)]

    mocker.patch.object(mapper.steps, '_analyse_interaction', side_effect=_side_effect)


def test_interaction_coi_extraction(add_datahub_interaction,):

    # Test mapping
    add_datahub_interaction(
        [{'id': '1', 'datahub_id': 'dsd-234', 'notes': 'Google exports to Brussels'}]
    )

    mapper.analyse_interactions()

    expected_rows = [(1, 'Brussels', 'Belgium', 'exported', 'GPE', ['export'], False)]
    assert rows_equal_query_results(
        expected_rows, f'SELECT * FROM "{mapper.output_schema}"."{mapper.output_table}"'
    )