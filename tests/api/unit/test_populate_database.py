import datetime
from unittest.mock import patch

import pytest
from flask import current_app as flask_app

from app.api.views import populate_database


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

    def test_reruns_task_if_last_was_successful(self, populate_database_task):
        sql = 'insert into etl_status (status, timestamp) values (%s, %s)'
        flask_app.dbi.execute_statement(sql, data=['SUCCESS', '2019-01-01 01:00'])
        with self.app.test_request_context():
            populate_database()
        populate_database_task.delay.assert_called_once()

    def test_called_with_specified_extractors_or_tasks(self, populate_database_task):
        with self.app.test_request_context() as request:
            request.request.args = {
                'extractors': 'datahub_company',
                'tasks': 'countries_of_interest',
            }
            output = populate_database()
        populate_database_task.delay.assert_called_once_with(
            False, ['datahub_company'], ['countries_of_interest']
        )
        assert output.get_json() == {
            'status': 200,
            'message': 'started populate_database task',
        }
