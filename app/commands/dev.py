from flask import current_app as app

from flask_script import Manager

import sqlalchemy_utils

from app.db.models import HawkUsers, sql_alchemy

DevCommand = Manager(app=app, usage='Development commands')


@DevCommand.option(
    '--create',
    dest='create',
    action='store_true',
    help='Create database using database name specified in (local) config',
)
@DevCommand.option(
    '--drop',
    dest='drop',
    action='store_true',
    help='Drop database using database name specified in (local) config',
)
@DevCommand.option(
    '--create_tables', dest='tables', action='store_true', help='Create database tables'
)
def db(create=False, drop=False, tables=False):
    if not drop and not create and not tables:
        print('please choose an option (--drop, --create or --create_tables)')
    db_url = app.config['SQLALCHEMY_DATABASE_URI']
    db_name = db_url.database
    if drop:
        print(f'Dropping {db_name} database')
        sqlalchemy_utils.drop_database(db_url)
    if create:
        print(f'Creating {db_name} database')
        sqlalchemy_utils.create_database(db_url, encoding='utf8')
    if create or tables:
        print('Creating DB tables')
        HawkUsers.__table__.create(sql_alchemy.engine, checkfirst=True)


@DevCommand.option(
    '--client_id', dest='client_id', type=str, help="a unique id for the client"
)
@DevCommand.option(
    '--client_key',
    dest='client_key',
    type=str,
    help="secret key only known by the client and server",
)
@DevCommand.option(
    '--client_scope',
    dest='client_scope',
    type=str,
    help="comma separated list of endpoints",
)
@DevCommand.option(
    '--description',
    dest='description',
    type=str,
    help="describe the usage of these credentials",
)
def add_hawk_user(client_id, client_key, client_scope, description):
    """
    Add hawk user
    """
    if not (client_id and client_key and client_scope and description):
        print(
            '(--client_id, --client_key and --client_scope, --description)'
            ' are mandatory parameter'
        )
    client_scope_list = client_scope.split(',')
    HawkUsers.add_user(
        client_id=client_id,
        client_key=client_key,
        client_scope=client_scope_list,
        description=description,
    )
