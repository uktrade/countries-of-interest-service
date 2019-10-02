import unittest
from db import query_db
from unittest.mock import patch, Mock

class TestQueryDB(unittest.TestCase):

    def test_executes_the_query_with_the_connection_cursor(self):
        connection = Mock()
        cursor = Mock()
        connection.cursor.return_value = cursor
        query = ''
        output = query_db(connection, query)
        cursor.execute.assert_called_once_with(query)

    def test_fetches_and_return_query_result(self):
        connection = Mock()
        cursor = Mock()
        connection.cursor.return_value = cursor
        query = ''
        expected = Mock()
        cursor.fetchall.return_value = expected
        output = query_db(connection, query)
        self.assertEqual(output, expected)
        
