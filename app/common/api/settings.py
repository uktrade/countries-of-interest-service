import datetime
import decimal
import json
import logging

import json_log_formatter
import numpy as np
import pandas as pd
from flask.json.provider import JSONProvider


class CustomJSONProvider(JSONProvider):
    def dumps(self, obj, **kwargs):
        return json.dumps(obj, **kwargs, cls=CustomJSONEncoder)

    def loads(self, s, **kwargs):
        return json.loads(s, **kwargs)


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if pd.isnull(obj):
            return None
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        return str(obj)


class JSONLogFormatter(json_log_formatter.JSONFormatter):
    def json_record(self, message, extra, record):
        extra['message'] = message
        if 'time' not in extra:
            extra['time'] = datetime.datetime.now()
        extra['level'] = record.levelname
        if record.levelno >= logging.ERROR:
            extra['lineno'] = record.lineno
            extra['filename'] = record.pathname
        return extra
