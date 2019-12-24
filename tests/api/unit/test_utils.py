import unittest
import unittest.mock

import pandas as pd

import app.api.utils as utils


class TestToCamelCase(unittest.TestCase):
    def test(self):
        before = 'john_smith'
        expected = 'johnSmith'
        after = utils.to_camel_case(before)
        self.assertEqual(after, expected)

    def test_leading_underscore(self):
        before = '_john_smith'
        expected = 'johnSmith'
        after = utils.to_camel_case(before)
        self.assertEqual(after, expected)

    def test_trailing_underscore(self):
        before = 'john_smith_'
        expected = 'johnSmith'
        after = utils.to_camel_case(before)
        self.assertEqual(after, expected)

    def test_multiple_underscores(self):
        before = 'john__smith'
        expected = 'johnSmith'
        after = utils.to_camel_case(before)
        self.assertEqual(after, expected)


class TestToWebDict(unittest.TestCase):
    @unittest.mock.patch('app.api.utils.to_records_web_dict')
    def test_if_record_orientation(self, to_records_web_dict):
        df = unittest.mock.Mock()
        utils.to_web_dict(df, orient='records')
        to_records_web_dict.assert_called_once_with(df)

    @unittest.mock.patch('app.api.utils.to_tabular_web_dict')
    def test_if_tabular_orientation(self, to_tabular_web_dict):
        df = unittest.mock.Mock()
        utils.to_web_dict(df, orient='tabular')
        to_tabular_web_dict.assert_called_once_with(df)

    def test_invalid_input(self):
        df = unittest.mock.Mock()
        with self.assertRaises(Exception):
            utils.to_web_dict(df, orient='invalid')


class TestToRecordsWebDict(unittest.TestCase):
    def test(self):
        df = pd.DataFrame([[0, 1, 2], [3, 4, 5]], columns=['col_1', 'col_2', 'col_3'])
        output = utils.to_records_web_dict(df)
        expected = {
            'results': [
                {
                    'col1': 0,
                    'col2': 1,
                    'col3': 2,
                },
                {
                    'col1': 3,
                    'col2': 4,
                    'col3': 5,
                }
            ]
        }
        self.assertEqual(output, expected)


class TestToTabularWebDict(unittest.TestCase):
    @unittest.mock.patch('app.api.utils.to_camel_case')
    def test(self, to_camel_case):
        df = unittest.mock.Mock(columns=['a', 'b', 'c'])
        df.values.tolist.return_value = [[0, 1, 2], [3, 4, 5]]
        output = utils.to_tabular_web_dict(df)
        expected = {
            'headers': [to_camel_case(c) for c in df.columns],
            'values': [[0, 1, 2], [3, 4, 5]],
        }
        self.assertEqual(output, expected)

    @unittest.mock.patch('app.api.utils.to_camel_case')
    def test_when_dataframe_has_only_one_column_make_values_a_list_not_of_list_of_lists(
        self, to_camel_case
    ):
        df = unittest.mock.Mock(columns=['a'])
        df.values.tolist.return_value = [[0], [3]]
        output = utils.to_tabular_web_dict(df)
        expected = {
            'headers': [to_camel_case(c) for c in df.columns],
            'values': [0, 3],
        }
        self.assertEqual(output, expected)


class TestResponseOrientationDecorator(unittest.TestCase):
    @unittest.mock.patch('app.api.utils.request')
    def test(self, request):
        request.args.get.return_value = 'your orientation'
        view = unittest.mock.Mock(__name__='asdf')
        wrapped = utils.response_orientation_decorator(view)

        self.assertEqual(wrapped.__name__, 'asdf')

        wrapped()
        request.args.get.assert_called_once_with('orientation', 'tabular')
        view.assert_called_once_with('your orientation')
