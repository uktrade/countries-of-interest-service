from app import app
from tests.TestCase import TestCase
from unittest.mock import Mock, patch
from etl.views import populate_database

@patch('etl.views.etl.tasks')
class TestPopulateDatabase(TestCase):

    def test_called_without_drop_table_in_request(self, tasks):
        with app.test_request_context():
            output = populate_database()
        tasks.populate_database.delay.assert_called_once_with(False)
        self.assertEqual(output, {'status': 200})

    def test_called_with_drop_table_in_request(self, tasks):
        with app.test_request_context() as request:
            request.request.args = {'drop-table': ''}
            output = populate_database()
        tasks.populate_database.delay.assert_called_once_with(True)
        self.assertEqual(output, {'status': 200})
