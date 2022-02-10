from unittest import mock

import pytest

from app.api.tasks import populate_database_task


class TestTasks:
    @pytest.mark.parametrize(
        'drop_table,extractors,tasks,expected_called_with_parameters',
        (
            (
                True,
                None,
                None,
                (True, [], []),
            ),
            (
                False,
                ['hello', 'test'],
                ['task'],
                (False, ['hello', 'test'], ['task']),
            ),
        ),
    )
    def test_populate_database_task(
        self,
        app_with_db,
        drop_table,
        extractors,
        tasks,
        expected_called_with_parameters,
    ):
        with mock.patch('app.etl.tasks.populate_database') as mock_populate_database:
            mock_populate_database.return_value = None
            populate_database_task(drop_table=drop_table, extractors=extractors, tasks=tasks)
        mock_populate_database.assert_called_once_with(*expected_called_with_parameters)
