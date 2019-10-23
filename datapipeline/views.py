from datapipeline.datapipeline import ( 
    populate_countries_and_sectors_of_interest, \
    populate_countries_of_interest
)

def populate_database():
    print('populate_database')
    output = populate_countries_and_sectors_of_interest()
    output = populate_countries_of_interest()
    return output
