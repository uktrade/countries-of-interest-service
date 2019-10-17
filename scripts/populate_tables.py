import psycopg2
from argparse import ArgumentParser
from etl.datahub_company_id_to_companies_house_company_number import Task \
    as DatahubCompanyIDToCompaniesHouseCompanyNumberTask
from etl.export_countries import Task as ExportCountriesTask
from etl.countries_of_interest import Task as CountriesOfInterestTask
from etl.countries_and_sectors_of_interest import Task as CountriesAndSectorsOfInterestTask
from etl.matched_companies import Task as MatchedCompaniesTask
from etl.sector_matches import Task as SectorMatchesTask
from etl.sectors_of_interest import Task as SectorsOfInterestTask
from etl.top_sectors import Task as TopSectorsTask
from etl.companies_with_orders import Task as CompaniesWithOrdersTask
from etl.companies_with_export_countries import Task as CompaniesWithExportCountriesTask
from etl.companies_with_countries_of_interest import Task as CompaniesWithCountriesOfInterestTask
from etl.order_frequency import Task as OrderFrequencyByDateTask
from etl.segments import Task as SegmentsTask
from utils.conduit import conduit_connect

parser = ArgumentParser()
parser.add_argument('--drop-tables', action='store_true', default=False, dest='drop_tables')
args = parser.parse_args()

#connection = conduit_connect('countries_of_interest_service_db', 'data-workspace-apps-dev')
connection = psycopg2.connect('postgresql://countries_of_interest_service@localhost'\
                              '/countries_of_interest_service')
connection_datahub = conduit_connect('datahub-dev-db', 'datahub-dev')

print('\n\n\033[31margs\033[0m')
print(args)

task = CountriesAndSectorsOfInterestTask(
    input_connection=connection_datahub,
    output_connection=connection,
    drop_table=args.drop_tables
)
print(task.sql)
print('\n\n\033[31m' + '=' * 75 + '\033[0m')
print('\033[31m===== TASK: {} =====\033[0m'.format(task.table_name))
print('\033[31m' + '=' * 75 + '\033[0m')
task()

task = CountriesOfInterestTask(
    input_connection=connection_datahub,
    output_connection=connection,
    drop_table=args.drop_tables
)
print('\n\n\033[31m' + '=' * 75 + '\033[0m')
print('\033[31m===== TASK: {} =====\033[0m'.format(task.table_name))
print('\033[31m' + '=' * 75 + '\033[0m')
task()

task = DatahubCompanyIDToCompaniesHouseCompanyNumberTask(
    input_connection=connection_datahub,
    output_connection=connection,
    drop_table=args.drop_tables
)

print('\n\n\033[31m' + '=' * 75 + '\033[0m')
print('\033[31m===== TASK: {} =====\033[0m'.format(task.table_name))
print('\033[31m' + '=' * 75 + '\033[0m')
task()

task = ExportCountriesTask(
    input_connection=connection_datahub,
    output_connection=connection,
    drop_table=args.drop_tables
)
print('\n\n\033[31m' + '=' * 75 + '\033[0m')
print('\033[31m===== TASK: {} =====\033[0m'.format(task.table_name))
print('\033[31m' + '=' * 75 + '\033[0m')
task()

task = SegmentsTask(
    input_connection=connection_datahub,
    output_connection=connection,
    drop_table=args.drop_tables
)
print('\n\n\033[31m' + '=' * 75 + '\033[0m')
print('\033[31m===== TASK: {} =====\033[0m'.format(task.table_name))
print('\033[31m' + '=' * 75 + '\033[0m')
task()

task = MatchedCompaniesTask(
    input_connection=connection_datahub,
    output_connection=connection,
    drop_table=args.drop_tables
)
print('\n\n\033[31m' + '=' * 75 + '\033[0m')
print('\033[31m===== TASK: {} =====\033[0m'.format(task.table_name))
print('\033[31m' + '=' * 75 + '\033[0m')
task()

task = SectorMatchesTask(
    input_connection=connection_datahub,
    output_connection=connection,
    drop_table=args.drop_tables
)
print('\n\n\033[31m' + '=' * 75 + '\033[0m')
print('\033[31m===== TASK: {} =====\033[0m'.format(task.table_name))
print('\033[31m' + '=' * 75 + '\033[0m')
task()

task = SectorsOfInterestTask(
    input_connection=connection_datahub,
    output_connection=connection,
    drop_table=args.drop_tables
)
print('\n\n\033[31m' + '=' * 75 + '\033[0m')
print('\033[31m===== TASK: {} =====\033[0m'.format(task.table_name))
print('\033[31m' + '=' * 75 + '\033[0m')
task()

task = TopSectorsTask(
    input_connection=connection_datahub,
    output_connection=connection,
    drop_table=args.drop_tables
)
print('\n\n\033[31m' + '=' * 75 + '\033[0m')
print('\033[31m===== TASK: {} =====\033[0m'.format(task.table_name))
print('\033[31m' + '=' * 75 + '\033[0m')
task()

task = CompaniesWithOrdersTask(
    input_connection=connection_datahub,
    output_connection=connection,
    drop_table=args.drop_tables
)
print('\n\n\033[31m' + '=' * 75 + '\033[0m')
print('\033[31m===== TASK: {} =====\033[0m'.format(task.table_name))
print('\033[31m' + '=' * 75 + '\033[0m')
task()

task = CompaniesWithExportCountriesTask(
    input_connection=connection_datahub,
    output_connection=connection,
    drop_table=args.drop_tables
)
print('\n\n\033[31m' + '=' * 75 + '\033[0m')
print('\033[31m===== TASK: {} =====\033[0m'.format(task.table_name))
print('\033[31m' + '=' * 75 + '\033[0m')
task()

task = CompaniesWithCountriesOfInterestTask(
    input_connection=connection_datahub,
    output_connection=connection,
    drop_table=args.drop_tables
)
print('\n\n\033[31m' + '=' * 75 + '\033[0m')
print('\033[31m===== TASK: {} =====\033[0m'.format(task.table_name))
print('\033[31m' + '=' * 75 + '\033[0m')
task()

task = OrderFrequencyByDateTask(
    input_connection=connection_datahub,
    output_connection=connection,
    drop_table=args.drop_tables
)
print('\n\n\033[31m' + '=' * 75 + '\033[0m')
print('\033[31m===== TASK: {} =====\033[0m'.format(task.table_name))
print('\033[31m' + '=' * 75 + '\033[0m')
task()
