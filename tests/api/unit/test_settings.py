import datetime

import numpy
import pytest

from app.api.settings import CustomJSONEncoder


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
