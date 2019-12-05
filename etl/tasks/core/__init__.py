import datetime

from db import get_db

from etl.tasks.core.countries_and_sectors_of_interest import (
    Task as PopulateCountriesAndSectorsOfInterestTask,
)
from etl.tasks.core.countries_of_interest import Task as PopulateCountriesOfInterestTask
from etl.tasks.core.export_countries import Task as ExportCountriesTask
from etl.tasks.core.sectors_of_interest import Task as SectorsOfInterestTask
from etl.tasks.core.source_data_extraction import (
    extract_datahub_company_dataset,
    extract_datahub_export_countries,
    extract_datahub_future_interest_countries,
    extract_datahub_interactions,
    extract_datahub_omis_dataset,
    extract_datahub_sectors,
    extract_export_wins,
)


def populate_database(drop_table):
    output = []
    with get_db() as connection:
        output.append(extract_datahub_company_dataset())
        output.append(extract_datahub_export_countries())
        output.append(extract_datahub_interactions())
        output.append(extract_datahub_future_interest_countries())
        output.append(extract_datahub_omis_dataset())
        output.append(extract_datahub_sectors())
        output.append(extract_export_wins())
        output.extend(
            [
                ExportCountriesTask(connection=connection, drop_table=drop_table)(),
                PopulateCountriesAndSectorsOfInterestTask(
                    connection=connection, drop_table=drop_table
                )(),
                PopulateCountriesOfInterestTask(
                    connection=connection, drop_table=drop_table
                )(),
                SectorsOfInterestTask(connection=connection, drop_table=drop_table)(),
            ]
        )
        with connection.cursor() as cursor:
            sql = 'create table if not exists etl_runs (timestamp timestamp)'
            cursor.execute(sql)
            sql = 'insert into etl_runs values (%s)'
            ts_finish = datetime.datetime.now()
            cursor.execute(sql, [ts_finish])
            sql = 'delete from etl_status'
            cursor.execute(sql)
            sql = '''insert into etl_status values (%s, %s)'''
            cursor.execute(sql, ['SUCCESS', ts_finish])
    return {'output': output}
