import datetime
from app import app
from db import get_db
from tests.TestCase import TestCase
from authentication import seen_nonce


class TestSeenNonce(TestCase):
    def test_nonce_not_seen(self):
        client_id = 'client_id'
        nonce = 'asdf'
        timestamp = '20190101'
        with app.app_context():
            self.assertFalse(seen_nonce(client_id, nonce, timestamp))

            with get_db() as connection:
                with connection.cursor() as cursor:
                    sql = 'select * from hawk_nonce'
                    cursor.execute(sql)
                    rows = cursor.fetchall()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], 'client_id')
        self.assertEqual(rows[0][1], 'asdf')
        self.assertEqual(rows[0][2], 20190101)

    def test_nonce_seen_before(self):
        client_id = 'client_id'
        nonce = 'asdf'
        timestamp = '2019-01-01 00:00'
        with app.app_context():
            with get_db() as connection:
                with connection.cursor() as cursor:
                    sql = (
                        'create table hawk_nonce ('
                        'client_id varchar(100), '
                        'nonce varchar(100), '
                        'timestamp timestamp'
                        ')'
                    )
                    cursor.execute(sql)
            with get_db() as connection:
                with connection.cursor() as cursor:
                    sql = 'insert into hawk_nonce values (%s, %s, %s)'
                    cursor.execute(sql, [client_id, nonce, timestamp])

            self.assertTrue(seen_nonce(client_id, nonce, timestamp))
