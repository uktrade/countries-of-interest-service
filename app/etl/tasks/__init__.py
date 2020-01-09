import datetime
import logging

from app.db.db_utils import execute_statement
from app.etl.tasks.countries_and_sectors_of_interest import (
    Task as PopulateCountriesAndSectorsOfInterestTask,
)
from app.etl.tasks.countries_of_interest import Task as PopulateCountriesOfInterestTask
from app.etl.tasks.country_standardisation import PopulateStandardisedCountriesTask
from app.etl.tasks.export_countries import Task as ExportToCountriesTask
from app.etl.tasks.sectors_of_interest import Task as SectorsOfInterestTask
from app.etl.tasks.source_data_extraction import (
    extract_countries_and_territories_reference_dataset,
    extract_datahub_company_dataset,
    extract_datahub_export_to_countries,
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
        extract_datahub_export_to_countries,
        extract_datahub_interactions,
        extract_datahub_future_interest_countries,
        extract_datahub_omis,
        # extract_datahub_sectors,
        # extract_export_wins,
    ]:
        logger.info(f'extractor: {extractor.__class__.__name__}')
        try:
            output = output + [extractor()]
        except Exception as e:
            output = output + [
                {
                    'table': extractor.model.__tablename__,
                    'status': 500,
                    'error': str(e),
                }
            ]

    for task in [
        PopulateStandardisedCountriesTask(),
        ExportToCountriesTask(drop_table=drop_table),
        PopulateCountriesAndSectorsOfInterestTask(drop_table=drop_table),
        PopulateCountriesOfInterestTask(drop_table=drop_table),
        SectorsOfInterestTask(drop_table=drop_table),
    ]:
        logger.info(f'task: {task.name}')
        try:
            output = output + [task()]
        except Exception as e:
            output = output + [
                {'table': task.table_name, 'status': 500, 'error': str(e)}
            ]
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
