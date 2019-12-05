import mohawk
from flask import request
from db import get_db
from utils.sql import query_database


def hawk_authenticate():
    url = request.url
    method = request.method
    content = request.data
    content_type = request.content_type
    mohawk.Receiver(
        lookup_hawk_credentials,
        request_header=request.headers['Authorization'],
        url=url,
        method=method,
        content=content,
        content_type=content_type,
        seen_nonce=seen_nonce,
    )


def hawk_decorator_factory(hawk_enabled):
    if hawk_enabled is True:
        return hawk_required
    else:
        return lambda view, *args, **kwargs: view


def hawk_required(view, *args, **kwargs):
    def wrapper(*args, **kwargs):
        try:
            hawk_authenticate()
        except Exception:
            return 'Hawk authentication failed', 401
        return view(*args, **kwargs)

    wrapper.__name__ = view.__name__
    return wrapper


def lookup_hawk_credentials(client_id):
    sql = 'select * from users where client_id=%s'
    connection = get_db()
    try:
        df = query_database(connection, sql, values=(client_id,))
        client_key = df['client_key'].values[0]
    except IndexError:
        raise LookupError()

    return {'id': client_id, 'key': client_key, 'algorithm': 'sha256'}


def seen_nonce(client_id, nonce, timestamp):
    with get_db() as connection:
        sql = (
            'create table if not exists hawk_nonce '
            '(client_id varchar(100), nonce varchar(100), timestamp int)'
        )
        with connection.cursor() as cursor:
            cursor.execute(sql)

    with get_db() as connection:
        sql = (
            'select * from hawk_nonce where '
            'client_id=%s and nonce=%s and timestamp=%s'
        )
        with connection.cursor() as cursor:
            cursor.execute(sql, [client_id, nonce, timestamp])
            seen = len(cursor.fetchall()) > 0

    if seen is True:
        return True
    else:
        with get_db() as connection:
            sql = 'insert into hawk_nonce values (%s, %s, %s)'
            with connection.cursor() as cursor:
                cursor.execute(sql, [client_id, nonce, timestamp])
        return False
