import mohawk
from flask import request

def hawk_authenticate():
    url = request.url
    method = request.method
    content = request.data
    content_type = request.content_type
    receiver = mohawk.Receiver(
        lookup_hawk_credentials,
        request_header=request.headers['Authorization'],
        url=url,
        method=method,
        content=content,
        content_type=content_type
    )

def hawk_decorator_factory(hawk_enabled):
    if hawk_enabled is True:
        return hawk_required
    else:
        return lambda view, *args, **kwargs: view

def lookup_hawk_credentials(user_id):
    sql = ''' select * from users where user_id='{user_id}' '''.format(user_id=user_id)
    connection = get_db()
    try:
        df = query_database(connection, sql)
        user_key = df['user_key'].values[0]
    except Exception as e:
        raise LookupError()
    
    return {
        'id': user_id,
        'key': user_key,
        'algorithm': 'sha256'
    }

def hawk_required(view, *args, **kwargs):
    def wrapper(*args, **kwargs):
        try:
            hawk_authenticate()
        except Exception:
            return 'Authentication failed', 401
        return view(*args, **kwargs)
    wrapper.__name__ = view.__name__
    return wrapper
