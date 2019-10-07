from argparse import ArgumentParser
from etl.datahub_company_id_to_companies_house_company_number import Task \
    as DatahubCompanyIDToCompaniesHouseCompanyNumberTask
from etl.export_countries import Task as ExportCountriesTask
from etl.countries_of_interest import Task as CountriesOfInterestTask
from etl.countries_and_sectors_of_interest import Task as CountriesAndSectorsOfInterestTask
from utils.conduit import conduit_connect
from utils.sql import execute_query, query_database

parser = ArgumentParser()
parser.add_argument('--drop-tables', action='store_true', default=False, dest='drop_tables')
args = parser.parse_args()

connection = conduit_connect('countries_of_interest_service_db', 'data-workspace-apps-dev')
connection_datahub = conduit_connect('datahub-dev-db', 'datahub-dev')

print('\n\n\033[31margs\033[0m')
print(args)

task = DatahubCompanyIDToCompaniesHouseCompanyNumberTask(
    input_connection=connection_datahub,
    output_connection=connection,
    drop_table=args.drop_tables
)
print('\n\n\033[31m===== TASK: {} =====\033[0m'.format(task.table_name))
task()

task = ExportCountriesTask(
    input_connection=connection_datahub,
    output_connection=connection,
    drop_table=args.drop_tables
)
print('\n\n\033[31m===== TASK: {} =====\033[0m'.format(task.table_name))
task()

task = CountriesOfInterestTask(
    input_connection=connection_datahub,
    output_connection=connection,
    drop_table=args.drop_tables
)
print('\n\n\033[31m===== TASK: {} =====\033[0m'.format(task.table_name))
task()

task = CountriesAndSectorsOfInterestTask(
    input_connection=connection_datahub,
    output_connection=connection,
    drop_table=args.drop_tables
)
print('\n\n\033[31m===== TASK: {} =====\033[0m'.format(task.table_name))
task()

