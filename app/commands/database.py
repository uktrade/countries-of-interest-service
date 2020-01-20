import click
from flask.cli import AppGroup


from app.etl.tasks.pipeline import EXTRACTORS, TASKS

cmd_group = AppGroup('database', help='Commands to extract data from external sources')


@cmd_group.command('populate')
@click.option(
    '--extractors',
    type=str,
    help=f"comma separated list of the following tasks: {', '.join(EXTRACTORS)}",
    default='',
)
@click.option(
    '--tasks',
    type=str,
    help=f"comma separated list of the following tasks: {', '.join(TASKS)}",
    default='',
)
@click.option(
    '--keep_tables', is_flag=True, help="Don't drop tables before inserting data",
)
def populate(keep_tables, extractors, tasks):
    from app.api.tasks import populate_database_task

    extractors = list(filter(None, extractors.split(',')))
    not_found = _check_parameters(extractors, EXTRACTORS)
    if not_found:
        return

    tasks = list(filter(None, tasks.split(',')))
    not_found = _check_parameters(tasks, TASKS)
    if not_found:
        return

    drop_table = not keep_tables
    populate_database_task(drop_table=drop_table, extractors=extractors, tasks=tasks)


def _check_parameters(user_entered, master_list):
    not_found = []
    for _entry in user_entered:
        if _entry not in master_list:
            not_found.append(_entry)
    if not_found:
        msg = (
            f'\nInvalid option: {", ".join(not_found)}\n\n'
            f'Must be one of the following: {", ".join(master_list)}\n'
        )
        click.echo(msg)
    return not_found
