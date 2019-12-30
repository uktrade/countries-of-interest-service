import datetime
import re
from decimal import Decimal
from uuid import UUID

from app.db.db_utils import execute_query

timezone_regex = re.compile(r'.*\d{2}:\d{2}(\+|-)\d{2}:\d{2}$')


def _query_result_to_rows(query):
    rows = execute_query(query, df=False)

    def _format(value):
        if isinstance(value, datetime.datetime):
            string_value = str(value)
            if timezone_regex.match(string_value):
                # Removing TZ info in string
                # (running tests in UTC timezone so no need to transform)
                r = string_value[:-6]
            else:
                r = string_value
        elif isinstance(value, datetime.date) or isinstance(value, UUID):
            r = str(value)
        elif isinstance(value, Decimal):
            r = float(value)
        else:
            r = value
        return r

    return [tuple(_format(value) for value in r) for r in rows]


def rows_equal_query_results(expected_rows, query, order_matters=False):
    db_rows = _query_result_to_rows(query)

    if len(expected_rows) != len(db_rows):
        print(
            'Number of rows are not equal:'
            f'{len(expected_rows)} expected, db has {len(db_rows)}'
        )
        return False

    if not order_matters:
        expected_rows.sort(key=_row_to_sortable_tuple)
        db_rows.sort(key=_row_to_sortable_tuple)

    return expected_rows == db_rows


def _row_to_sortable_tuple(row):
    return tuple(('**none**' if e is None else e) for e in row)
