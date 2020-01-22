import click
from flask.cli import AppGroup

from app.utils.csv import get_coi_csv, get_interactions_csv

cmd_group = AppGroup('csv', help='Commands to generate csv')


@cmd_group.command('generate_coi_csv')
@click.option(
    '--output-filename', type=str, help=f"CSV file name", default='coi_data.csv',
)
@click.option(
    '--limit', type=int, help=f"Row limit", default=None,
)
def generate_coi_csv(output_filename, limit):
    with open(output_filename, 'w') as f:
        get_coi_csv(f, limit=limit)
    click.echo(f'\n{output_filename} generated')


@cmd_group.command('generate_interactions_csv')
@click.option(
    '--output-filename', type=str, help=f"CSV file name", default='interactions.csv',
)
@click.option(
    '--limit', type=int, help=f"Row limit", default=None,
)
@click.option(
    '--order_by',
    type=str,
    help=f"Order by must be either timestamp or random. Default: timestamp",
    default='timestamp',
)
def generate_interactions_csv(output_filename, order_by, limit):
    if order_by not in ['timestamp', 'random']:
        ctx = click.get_current_context()
        click.echo(ctx.get_help())
    else:

        with open(output_filename, 'w') as f:
            get_interactions_csv(f, order_by=order_by, limit=limit)
        click.echo(f'\n{output_filename} generated')
