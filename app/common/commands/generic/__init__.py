from flask.cli import AppGroup

from .hawk_api_request import hawk_api_request as hawk_api_request_command

cmd_group = AppGroup('generic', help='Generic commands')

cmd_group.add_command(hawk_api_request_command)
