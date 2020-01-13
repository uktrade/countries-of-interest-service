from unittest import mock

import pytest

from app.commands.dev import DevCommand


class TestDevCommand:
    def test_cmd_config(self, app_with_db):
        DevCommand(app_with_db)
        assert list(DevCommand._commands.keys()) == ['db', 'add_hawk_user']

        cmd = DevCommand._commands['db']
        options = self.get_option_names_from_cmd(cmd)
        assert set(options) == {
            '--drop',
            '--create_tables',
            '--drop_tables',
            '--create',
            '--recreate_tables',
        }

        cmd = DevCommand._commands['add_hawk_user']
        options = self.get_option_names_from_cmd(cmd)
        assert set(options) == {
            '--client_key',
            '--client_scope',
            '--description',
            '--client_id',
        }

    def get_option_names_from_cmd(self, cmd):
        options = []
        for option in cmd.option_list:
            options.append(option.args[0])
        return options

    @pytest.mark.parametrize(
        'client_id,expected_add_user_called', (('client_id', True), (None, False),),
    )
    @mock.patch('app.db.models.internal.HawkUsers.add_user')
    def test_run_hawk_user(
        self, mock_add_user, client_id, expected_add_user_called, app_with_db, capsys
    ):
        mock_add_user.return_value = None
        cmd = DevCommand._commands['add_hawk_user']
        cmd.run(
            client_id=client_id,
            client_key='client_key',
            client_scope='client_scope',
            description='description',
        )
        assert mock_add_user.called is expected_add_user_called
        if not expected_add_user_called:
            stdout, _ = capsys.readouterr()
            assert stdout == (
                '(--client_id, --client_key and --client_scope, --description)'
                ' are mandatory parameter\n'
            )

    @pytest.mark.parametrize(
        'drop_database,create_tables,drop_tables,'
        'create_database,recreate_tables,expected_msg',
        (
            (False, False, False, False, False, True),
            (True, False, False, False, False, False),
            (False, True, False, False, False, False),
            (False, False, True, False, False, False),
            (False, False, False, True, False, False),
            (False, False, False, False, True, False),
        ),
    )
    @mock.patch('flask_sqlalchemy.SQLAlchemy.create_all')
    @mock.patch('flask_sqlalchemy.SQLAlchemy.drop_all')
    @mock.patch('sqlalchemy_utils.create_database')
    @mock.patch('sqlalchemy_utils.drop_database')
    def test_run_db(
        self,
        mock_drop_database,
        mock_create_database,
        mock_drop_tables,
        mock_create,
        drop_database,
        create_tables,
        drop_tables,
        create_database,
        recreate_tables,
        expected_msg,
        app_with_db,
        capsys,
    ):
        mock_drop_database.return_value = None
        mock_create_database.return_value = None
        mock_drop_tables.return_value = None
        mock_create_database.return_value = None

        cmd = DevCommand._commands['db']
        cmd.run(
            drop=drop_database,
            drop_tables=drop_tables,
            create=create_database,
            tables=create_tables,
            recreate_tables=recreate_tables,
        )
        assert mock_drop_database.called is drop_database
        assert mock_create_database.called is create_database
        assert mock_drop_tables.called is any([drop_tables, recreate_tables])
        assert mock_create.called is any(
            [create_tables, create_database, recreate_tables]
        )

        if expected_msg:
            stdout, _ = capsys.readouterr()
            assert stdout == (
                "please choose an option"
                " (--drop, --create, --create_tables, "
                "--drop_tables or --recreate_tables)\n"
            )
