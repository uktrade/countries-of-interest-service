import datetime
import logging

from app.db.db_utils import execute_statement
from app.etl.tasks.pipeline import (
    EXTRACTORS,
    EXTRACTORS_DICT,
    TASKS,
    TASKS_DICT,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def populate_database(drop_table, extractors, tasks):
    logger.info(f'populate_database')
    if not extractors and not tasks:
        extractors = EXTRACTORS
        tasks = TASKS

    if extractors:
        logger.info(f'Running {", ".join(extractors)} extractors')
    if tasks:
        logger.info(f'Running {", ".join(tasks)} tasks')

    output = []
    for name, extractor in EXTRACTORS_DICT.items():
        if name in extractors:
            logger.info(f'extractor: {extractor.__class__.__name__}')
            try:
                output = output + [extractor()]
            except Exception as e:
                print(extractor)
                output = output + [
                    {
                        'table': extractor.model.__tablename__,
                        'status': 500,
                        'error': str(e),
                    }
                ]

    for name, task in TASKS_DICT.items():
        if name in tasks:
            subtask_list = [task] if not isinstance(task, list) else task
            for position, subtask in enumerate(subtask_list):
                subtask = subtask(drop_table=drop_table if position == 0 else False)
                logger.info(f'task: {subtask.name}')
                try:
                    output = output + [subtask()]
                except Exception as e:
                    print(subtask)
                    output = output + [
                        {'table': subtask.table_name, 'status': 500, 'error': str(e)}
                    ]

    sql = 'insert into etl_runs (timestamp) values (%s)'
    ts_finish = datetime.datetime.now()
    execute_statement(sql, [ts_finish])
    sql = 'delete from etl_status'
    execute_statement(sql)
    sql = '''insert into etl_status (status, timestamp) values (%s, %s)'''
    execute_statement(sql, ['SUCCESS', ts_finish])

    output = {'output': output}
    logger.info(output)
    return output
