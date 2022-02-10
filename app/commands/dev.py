import click
import sqlalchemy_utils
from data_engineering.common.db.models import HawkUsers
from flask import current_app as flask_app
from flask.cli import AppGroup, with_appcontext

from app.db.models import get_schemas


cmd_group = AppGroup('dev', help='Commands to build database')


@cmd_group.command('db')
@with_appcontext
@click.option(
    '--create',
    is_flag=True,
    help='Create database using database name specified in (local) config',
)
@click.option(
    '--drop',
    is_flag=True,
    help='Drop database using database name specified in (local) config',
)
@click.option('--create_tables', is_flag=True, help='Create database tables')
@click.option(
    '--drop_tables',
    is_flag=True,
    help='Drop database tables',
)
@click.option(
    '--recreate_tables',
    is_flag=True,
    help='Drop and recreate database tables',
)
def db(create, drop, drop_tables, create_tables, recreate_tables):
    """
    Create/Drop database or database tables
    """
    if not any([create, drop, drop_tables, create_tables, recreate_tables]):
        ctx = click.get_current_context()
        click.echo(ctx.get_help())
    else:
        db_url = flask_app.config['SQLALCHEMY_DATABASE_URI']
        db_name = db_url.database
        if drop:
            click.echo(f'Dropping {db_name} database')
            sqlalchemy_utils.drop_database(db_url)
        if create:
            click.echo(f'Creating {db_name} database')
            sqlalchemy_utils.create_database(db_url, encoding='utf8')
        if drop_tables or recreate_tables:
            click.echo('Drop DB tables')
            drop_schemas()
        if create or create_tables or recreate_tables:
            click.echo('Creating DB tables')
            flask_app.db.create_all()


def drop_schemas():
    schemas = get_schemas()
    for schema in schemas:
        flask_app.dbi.drop_schema(schema)


@cmd_group.command('add_hawk_user')
@click.option('--client_id', type=str, help="a unique id for the client")
@click.option(
    '--client_key',
    type=str,
    help="secret key only known by the client and server",
)
@click.option(
    '--client_scope',
    type=str,
    help="comma separated list of endpoints",
)
@click.option(
    '--description',
    type=str,
    help="describe the usage of these credentials",
)
def add_hawk_user(client_id, client_key, client_scope, description):
    """
    Add hawk user
    """
    if not all([client_id, client_key, client_scope, description]):
        click.echo('All parameters are required')
        ctx = click.get_current_context()
        click.echo(ctx.get_help())
    else:
        client_scope_list = client_scope.split(',')
        HawkUsers.add_user(
            client_id=client_id,
            client_key=client_key,
            client_scope=client_scope_list,
            description=description,
        )
