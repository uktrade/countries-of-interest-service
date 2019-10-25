from db import get_db
from etl.extraction import (
    extract_datahub_company_dataset,
    extract_datahub_omis_dataset,
    extract_datahub_future_interest_countries,
    extract_datahub_export_countries,
    extract_datahub_sectors,
)
from etl.countries_and_sectors_of_interest import Task as PopulateCountriesAndSectorsOfInterestTask
from etl.countries_of_interest import Task as PopulateCountriesOfInterestTask
from etl.datahub_company_id_to_companies_house_company_number import Task \
    as DatahubCompanyIDToCompaniesHouseCompanyNumberTask
from etl.export_countries import Task as ExportCountriesTask
from etl.sectors_of_interest import Task as SectorsOfInterestTask


def populate_database():
    connection = get_db()
    output = []
    output.append(extract_datahub_company_dataset())
    output.append(extract_datahub_omis_dataset())
    output.append(extract_datahub_future_interest_countries())
    output.append(extract_datahub_export_countries())
    output.append(extract_datahub_sectors())
    output.extend(
        [
            PopulateCountriesAndSectorsOfInterestTask(connection=connection)(),
            PopulateCountriesOfInterestTask(connection=connection)(),
            DatahubCompanyIDToCompaniesHouseCompanyNumberTask(connection=connection)(),
            ExportCountriesTask(connection=connection)(),
            SectorsOfInterestTask(connection=connection)(),
        ]
    )
    return {'output': output}
