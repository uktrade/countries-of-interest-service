import datetime
from db import get_db
from etl.extraction import (
    extract_datahub_company_dataset,
    extract_datahub_omis_dataset,
    extract_datahub_future_interest_countries,
    extract_datahub_export_countries,
    extract_datahub_sectors,
    extract_export_wins,
    extract_datahub_interactions,
)
from etl.countries_and_sectors_of_interest import Task as PopulateCountriesAndSectorsOfInterestTask
from etl.countries_of_interest import Task as PopulateCountriesOfInterestTask
from etl.datahub_company_id_to_companies_house_company_number import Task \
    as DatahubCompanyIDToCompaniesHouseCompanyNumberTask
from etl.export_countries import Task as ExportCountriesTask
from etl.sectors_of_interest import Task as SectorsOfInterestTask


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
                DatahubCompanyIDToCompaniesHouseCompanyNumberTask(
                    connection=connection,
                    drop_table=drop_table
                )(),
                ExportCountriesTask(connection=connection, drop_table=drop_table)(),
                PopulateCountriesAndSectorsOfInterestTask(
                    connection=connection,
                    drop_table=drop_table
                )(),
                PopulateCountriesOfInterestTask(connection=connection, drop_table=drop_table)(),
                SectorsOfInterestTask(connection=connection, drop_table=drop_table)(),
            ]
        )
        with connection.cursor() as cursor:
            sql = 'create table if not exists etl_runs (timestamp timestamp)'
            cursor.execute(sql)
            sql = 'insert into etl_runs values (%s)'
            cursor.execute(sql, [datetime.datetime.now()])
    return {'output': output}
