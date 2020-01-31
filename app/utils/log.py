from functools import wraps

from flask import current_app as flask_app

process_depth = -1


def write(description):
    def dec(f):
        @wraps(f)
        def outer(*args, **kwargs):
            global process_depth
            process_depth += 1
            prefix = '  ' * process_depth
            flask_app.logger.debug(f'{prefix}{description} starting...')
            rv = f(*args, **kwargs)
            flask_app.logger.debug(f'{prefix}{description} done.')
            process_depth -= 1
            return rv

        return outer

    return dec
