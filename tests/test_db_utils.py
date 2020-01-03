import unittest.mock

import app.db.db_utils


class TestInsertData(unittest.TestCase):
    @unittest.mock.patch('app.db.db_utils.execute_statement')
    def test_if_dataframe_is_empty_do_nothing(self, execute_statement):
        df = []
        output = app.db.db_utils.insert_data(df, 'my_table')
        self.assertEqual(output, None)
        execute_statement.assert_not_called()
