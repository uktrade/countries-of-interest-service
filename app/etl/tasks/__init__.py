import datetime

from flask import current_app as flask_app

from app.etl.tasks.pipeline import (
    EXTRACTORS,
    EXTRACTORS_LIST,
    TASKS,
    TASKS_DICT,
)


def populate_database(drop_table, extractors, tasks):
    flask_app.logger.info('populate_database')
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

    ts_finish = datetime.datetime.now()
    flask_app.dbi.execute_statement(
        'insert into etl_runs (timestamp) values (:finish)', [{"finish": ts_finish}]
    )
    flask_app.dbi.execute_statement('delete from etl_status')
    flask_app.dbi.execute_statement(
        'insert into etl_status (status, timestamp) values (:status, :finish)',
        [{"status": 'SUCCESS', "finish": ts_finish}],
    )

    output = {'output': output}
    pretty_log_output(output)
    return output


def pretty_log_output(output):
    for log_entry in output['output']:
        flask_app.logger.info('', extra=log_entry)
