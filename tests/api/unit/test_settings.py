import datetime
import json
import logging

import numpy
import pytest
from freezegun import freeze_time

from app.api.settings import CustomJSONEncoder, JSONLogFormatter


@pytest.mark.parametrize(
    'data,expected_result',
    (
        (None, None),
        (numpy.int32(1), 1),
        (numpy.float32(1.5), 1.5),
        (numpy.array([1, 2, 3]), [1, 2, 3]),
        ('hello', "hello"),
        (datetime.datetime(2020, 1, 1), "2020-01-01T00:00:00"),
    ),
)
def test_json_encoder(data, expected_result):
    assert CustomJSONEncoder().default(data) == expected_result


@freeze_time('2020-02-02 10:00:00')
def test_log_formatting():
    record = logging.LogRecord('', logging.ERROR, 'test.py', 1, 'hello', None, '')
    result = JSONLogFormatter()
    assert json.loads(result.format(record)) == {
        "message": "hello",
        "time": "2020-02-02T10:00:00",
        "level": "ERROR",
        "filename": "test.py",
        'lineno': 1,
    }
