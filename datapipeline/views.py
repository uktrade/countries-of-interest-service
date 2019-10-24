from datapipeline.datapipeline import ( 
    populate_countries_and_sectors_of_interest,
    populate_countries_of_interest,
    populate_datahub_company_id_to_companies_house_company_number,
    populate_export_countries,
    populate_sectors,
    populate_sectors_of_interest,
)

def get_data_report_data():
    data = {
        'dataReportData': {
            'headers': ['name', 'value'],
            'values': [
                ['nCompanies', 1000],
                ['nCompaniesMatchedToCompaniesHouse', 10],
                ['nCompaniesMatchedToSector', 90],
                ['nCompaniesMatchedToDuplicateCompaniesHouseCompany', 2],
                ['nSectors', 200],
                ['nCompaniesWithOmisOrders', 3],
                ['nCompaniesWithExportCountries', 4],
                ['nCompaniesWithFutureInterestCountries', 5]
            ]
        },
        'topSectors': {
            'headers': ['sector', 'count'],
            'values': [
                ['retail', 500],
                ['food', 200],
            ]
        },
        'omisOrderFrequency': {
            'headers': ['date', 'frequency'],
            'values': [
                ['2018-01-01', 3],
                ['2018-01-02', 10],
                ['2018-01-03', 25],
            ]
        }
    }
    return data
    

def populate_database():
    output = []
    output.append(populate_countries_and_sectors_of_interest())
    output.append(populate_countries_of_interest())
    output.append(populate_datahub_company_id_to_companies_house_company_number())
    output.append(populate_export_countries())
    output.append(populate_sectors())
    output.append(populate_sectors_of_interest())
    return {'output': output}
