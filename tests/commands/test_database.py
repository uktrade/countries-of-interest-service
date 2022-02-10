from unittest import mock

import pytest

from app.commands.database import populate


class TestDatabaseCommand:
    def test_cmd_help(self, app_with_db):
        runner = app_with_db.test_cli_runner()
        result = runner.invoke(populate, ['--help'])
        assert 'Usage: populate [OPTIONS]' in result.output
        assert result.exit_code == 0
        assert result.exception is None

    @pytest.mark.parametrize(
        'keep_tables,extractors,tasks,expected_called_task_with,expected_msg',
        (
            (
                True,
                '',
                '',
                (False, [], []),
                '',
            ),
            (
                False,
                '',
                '',
                (True, [], []),
                '',
            ),
            (
                False,
                'hello',
                '',
                False,
                'Invalid option: hello',
            ),
            (
                False,
                '',
                'goodbye',
                False,
                'Invalid option: goodbye',
            ),
        ),
    )
    def test_run_cmd(
        self,
        app_with_db,
        keep_tables,
        extractors,
        tasks,
        expected_called_task_with,
        expected_msg,
    ):
        runner = app_with_db.test_cli_runner()
        with mock.patch('app.etl.tasks.populate_database') as mock_populate_database_task:
            mock_populate_database_task.return_value = []
            args = []
            if keep_tables:
                args.append('--keep_tables')
            if extractors:
                args.extend(['--extractors', extractors])
            if tasks:
                args.extend(['--tasks', tasks])
            result = runner.invoke(populate, args)
        if expected_called_task_with:
            mock_populate_database_task.assert_called_once_with(*expected_called_task_with)
        else:
            assert mock_populate_database_task.called is False
            assert expected_msg in result.output
