import datetime
import logging

from app.db.db_utils import execute_statement
from app.etl.tasks.core.countries_and_sectors_of_interest import (
    Task as PopulateCountriesAndSectorsOfInterestTask,
)
from app.etl.tasks.core.countries_of_interest import (
    Task as PopulateCountriesOfInterestTask,
)
from app.etl.tasks.core.export_countries import Task as ExportCountriesTask
from app.etl.tasks.core.sectors_of_interest import Task as SectorsOfInterestTask
from app.etl.tasks.core.source_data_extraction import (
    extract_countries_and_territories_reference_dataset,
    extract_datahub_company_dataset,
    extract_datahub_export_countries,
    extract_datahub_future_interest_countries,
    extract_datahub_interactions,
    extract_datahub_omis,
    # extract_datahub_sectors,
    # extract_export_wins,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def populate_database(drop_table):
    logger.info(f'populate_database')
    output = []
    for extractor in [
        extract_countries_and_territories_reference_dataset,
        extract_datahub_company_dataset,
        extract_datahub_export_countries,
        extract_datahub_interactions,
        extract_datahub_future_interest_countries,
        extract_datahub_omis,
        # extract_datahub_sectors,
        # extract_export_wins,
    ]:
        logger.info(f'extractor: {extractor.__class__.__name__}')
        output = output + [extractor()]

    for task in [
        ExportCountriesTask(drop_table=drop_table),
        PopulateCountriesAndSectorsOfInterestTask(drop_table=drop_table),
        PopulateCountriesOfInterestTask(drop_table=drop_table),
        SectorsOfInterestTask(drop_table=drop_table),
    ]:
        logger.info(f'task: {task.name}')
        output = output + [task()]
    sql = 'create table if not exists etl_runs (timestamp timestamp)'
    execute_statement(sql)
    sql = 'insert into etl_runs values (%s)'
    ts_finish = datetime.datetime.now()
    execute_statement(sql, [ts_finish])
    sql = 'delete from etl_status'
    execute_statement(sql)
    sql = '''insert into etl_status values (%s, %s)'''
    execute_statement(sql, ['SUCCESS', ts_finish])

    output = {'output': output}
    logger.info(output)
    return output
