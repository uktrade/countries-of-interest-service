import pandas as pd
from unittest.mock import patch
from tests.TestCase import TestCase
from etl.extraction import extract_export_wins
from utils.sql import query_database
from app import app
from db import get_db

class TestExtractExportWins(TestCase):

    @patch('etl.extraction.requests')
    def test(self, requests):
        data = {
            'headers': ['id', 'companyId', 'timestamp'],
            'values': [
                ['23f66b0e-05be-40a5-9bf2-fa44dc7714a8', 'asdf', 'IT', '2019-01-01 01:00:00'],
                ['f50d892d-388a-405b-9e30-16b9971ac0d4', 'ffff', 'GO', '2019-01-02 18:00:00']
            ]
        }
        requests.get.json.return_value = data
        with app.app_context():
            output = extract_export_wins()

        sql = ''' select * from export_wins '''
        with app.app_context():
            connection = get_db()
        df = query_database(connection, sql)
        df['timestamp'] = df['timestamp'].astype(str)
        df_expected = pd.DataFrame(data['values'])
        self.assertTrue((df.values == df_expected.values).all())
