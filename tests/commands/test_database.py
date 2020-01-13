from unittest import mock

import pytest

from app.commands.database import PopulateDatabaseCommand


class TestDatabaseCommand:
    def test_cmd_config(self, app_with_db):
        PopulateDatabaseCommand(app_with_db)
        assert list(PopulateDatabaseCommand._commands.keys()) == ['populate']

        cmd = PopulateDatabaseCommand._commands['populate']
        options = self.get_option_names_from_cmd(cmd)
        assert set(options) == {'--keep_tables', '--tasks', '--extractors'}

    def get_option_names_from_cmd(self, cmd):
        options = []
        for option in cmd.option_list:
            options.append(option.args[0])
        return options

    @pytest.mark.parametrize(
        'keep_tables,extractors,tasks,expected_called_task_with,expected_msg',
        (
            (True, '', '', (False, [], []), '',),
            (False, '', '', (True, [], []), '',),
            (False, 'hello', '', False, 'Invalid option: hello',),
            (False, '', 'goodbye', False, 'Invalid option: goodbye',),
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
        capsys,
    ):
        PopulateDatabaseCommand(app_with_db)
        cmd = PopulateDatabaseCommand._commands['populate']

        with mock.patch(
            'app.etl.tasks.populate_database'
        ) as mock_populate_database_task:
            mock_populate_database_task.return_value = []
            cmd.run(keep_tables=keep_tables, extractors=extractors, tasks=tasks)
        if expected_called_task_with:
            mock_populate_database_task.assert_called_once_with(
                *expected_called_task_with
            )
        else:
            assert mock_populate_database_task.called is False
            stdout, _ = capsys.readouterr()
            assert expected_msg in stdout
