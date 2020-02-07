import datetime

from flask import current_app as flask_app

from app.db.db_utils import execute_statement
from app.etl.tasks.pipeline import (
    EXTRACTORS,
    EXTRACTORS_LIST,
    TASKS,
    TASKS_DICT,
)


def populate_database(drop_table, extractors, tasks):
    flask_app.logger.info(f'populate_database')
    if not extractors and not tasks:
        extractors = EXTRACTORS
        tasks = TASKS

    if extractors:
        flask_app.logger.info(f'Running {", ".join(extractors)} extractors')
    if tasks:
        flask_app.logger.info(f'Running {", ".join(tasks)} tasks')

    output = []
    for extractor in EXTRACTORS_LIST:
        name = extractor.name
        if name in extractors:
            flask_app.logger.info(f'Running extractor: {name}')
            extractor = extractor()
            try:
                output = output + [extractor()]
            except Exception as e:
                output = output + [{'extractor': name, 'status': 500, 'error': str(e)}]

    for name, task in TASKS_DICT.items():
        if name in tasks:
            subtask_list = [task] if not isinstance(task, list) else task
            for position, subtask in enumerate(subtask_list):
                subtask = subtask(drop_table=drop_table if position == 0 else False)
                flask_app.logger.info(f'Running task: {subtask.name}')
                try:
                    output = output + [subtask()]
                except Exception as e:
                    output = output + [{'task': subtask.name, 'status': 500, 'error': str(e)}]

    sql = 'insert into etl_runs (timestamp) values (%s)'
    ts_finish = datetime.datetime.now()
    execute_statement(sql, [ts_finish])
    sql = 'delete from etl_status'
    execute_statement(sql)
    sql = '''insert into etl_status (status, timestamp) values (%s, %s)'''
    execute_statement(sql, ['SUCCESS', ts_finish])

    output = {'output': output}
    pretty_log_output(output)
    return output


def pretty_log_output(output):
    for log_entry in output['output']:
        flask_app.logger.info('', extra=log_entry)
