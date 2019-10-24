from datapipeline.datapipeline import ( 
    populate_countries_and_sectors_of_interest,
    populate_countries_of_interest,
    populate_datahub_company_id_to_companies_house_company_number,
    populate_export_countries,
    populate_sectors,
    populate_sectors_of_interest,
)

def populate_database():
    print('populate_database')
    output = populate_countries_and_sectors_of_interest()
    output = populate_countries_of_interest()
    output = populate_datahub_company_id_to_companies_house_company_number()
    output = populate_export_countries()
    output = populate_sectors()
    output = populate_sectors_of_interest()
    return output
