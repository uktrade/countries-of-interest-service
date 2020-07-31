import datetime
from unittest.mock import patch

import pytest
from flask import current_app as flask_app

from app.api.views import get_index, populate_database


@patch('app.api.views.populate_database_task')
class TestPopulateDatabase:
    @pytest.fixture(autouse=True)
    def setup(self, app_with_db):
        app_with_db.config['access_control']['hawk_enabled'] = False
        self.app = app_with_db

    def test_called_without_drop_table_in_request(self, populate_database_task):
        with self.app.test_request_context():
            output = populate_database()
        populate_database_task.delay.assert_called_once_with(False, [], [])
        assert output.get_json() == {
            'status': 200,
            'message': 'started populate_database task',
        }

    def test_called_with_drop_table_in_request(self, populate_database_task):

        with self.app.test_request_context() as request:
            request.request.args = {'drop-table': ''}
            output = populate_database()
        populate_database_task.delay.assert_called_once_with(True, [], [])

        assert output.get_json() == {
            'status': 200,
            'message': 'started populate_database task',
        }

    @patch('app.api.views.datetime')
    def test_creates_etl_status_table_if_it_does_not_exist(
        self, mock_datetime, populate_database_task
    ):
        mock_datetime.datetime.now.return_value = datetime.datetime(2019, 1, 1, 1)
        with self.app.test_request_context() as request:
            request.request.args = {'drop-table': ''}
            populate_database()
        sql = 'select * from etl_status'
        rows = flask_app.dbi.execute_query(sql, df=False)
        populate_database_task.delay.assert_called_once()
        assert rows[0][1] == 'RUNNING'
        assert rows[0][2] == datetime.datetime(2019, 1, 1, 1)

    def test_if_force_update_rerun_task(self, populate_database_task):
        with self.app.test_request_context() as request:
            request.request.args = {'force-update': ''}
            populate_database()
        populate_database_task.delay.assert_called_once()

    def test_if_force_update_rerun_while_another_task_is_running(self, populate_database_task):
        sql = 'insert into etl_status (status, timestamp) values (%s, %s)'
        flask_app.dbi.execute_statement(sql, data=['RUNNING', '2019-01-01 01:00'])
        with self.app.test_request_context() as request:
            request.request.args = {'force-update': ''}
            populate_database()
        populate_database_task.delay.assert_called_once()

    def test_does_not_rerun_while_another_task_is_running(self, populate_database_task):
        sql = 'insert into etl_status (status, timestamp) values (%s, %s)'
        flask_app.dbi.execute_statement(sql, data=['RUNNING', '2019-01-01 01:00'])
        with self.app.test_request_context():
            populate_database()
        populate_database_task.delay.assert_not_called()

    def test_reruns_task_if_last_was_succesful(self, populate_database_task):
        sql = 'insert into etl_status (status, timestamp) values (%s, %s)'
        flask_app.dbi.execute_statement(sql, data=['SUCCESS', '2019-01-01 01:00'])
        with self.app.test_request_context():
            populate_database()
        populate_database_task.delay.assert_called_once()


class TestGetIndex:
    @pytest.fixture(autouse=True)
    def setup(self, app_with_db):
        self.app = app_with_db

    @patch('app.sso.token.is_authenticated')
    @patch('app.api.views.render_template')
    def test_last_updated_passed_to_template(self, render_template, is_authenticated):
        is_authenticated.return_value = True
        sql = 'insert into etl_runs (timestamp) values (%s)'
        values = [
            ('2019-01-01 01:00',),
            ('2019-01-01 02:00',),
        ]
        flask_app.dbi.execute_statement(sql, values)
        with self.app.test_client() as c:
            with c.session_transaction() as sess:
                sess['_authbroker_token'] = 'Test'
                with self.app.test_request_context():
                    get_index()
        assert is_authenticated.called
        render_template.assert_called_once_with(
            'index.html', last_updated='2019-01-01 02:00:00',
        )

    @patch('app.sso.token.is_authenticated')
    @patch('app.api.views.render_template')
    def test_returns_message_if_there_are_no_prior_runs(self, render_template, is_authenticated):
        is_authenticated.return_value = True
        with self.app.test_request_context():
            get_index()
        assert is_authenticated.called
        render_template.assert_called_once_with(
            'index.html', last_updated='Database not yet initialised',
        )
