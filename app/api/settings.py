import datetime

from flask.json import JSONEncoder

import numpy as np

import pandas as pd


class CustomJSONEncoder(JSONEncoder):
    def default(self, obj):
        if pd.isnull(obj):
            return None
        elif isinstance(obj, datetime.datetime):
            return obj.isoformat()
        elif isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return str(obj)
