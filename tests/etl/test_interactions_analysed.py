import unittest
from unittest import mock

from app.config.constants import Task as TaskConstant
from app.etl.tasks.interactions_analysed import Task


class TestTask(unittest.TestCase):
    @mock.patch('app.etl.tasks.interactions_analysed.analyse_interactions')
    def test_call(self, analyse_interactions):
        task = Task()
        output = task()

        analyse_interactions.assert_called_once_with()
        expected = {
            'status': 200,
            'task': TaskConstant.INTERACTIONS_ANALYSED.value,
        }
        self.assertEqual(output, expected)
