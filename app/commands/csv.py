import click
from flask.cli import AppGroup

from app.utils.csv import get_coi_csv

cmd_group = AppGroup('csv', help='Commands to generate csv')


@cmd_group.command('generate_coi_csv')
@click.option(
    '--output-filename',
    type=str,
    help="CSV file name",
    default='coi_data.csv',
)
@click.option(
    '--limit',
    type=int,
    help="Row limit",
    default=None,
)
def generate_coi_csv(output_filename, limit):
    with open(output_filename, 'w') as f:
        get_coi_csv(f, limit=limit)
    click.echo(f'\n{output_filename} generated')
