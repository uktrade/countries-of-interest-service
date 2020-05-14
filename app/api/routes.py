from app.api.views import (
    data_visualisation,
    get_company_countries_and_sectors_of_interest,
    get_data_visualisation_data,
    get_index,
    populate_database,
)


RULES = [
    (
        '/api/v1/get-company-countries-and-sectors-of-interest',
        get_company_countries_and_sectors_of_interest,
    ),
    ('/', get_index,),
    ('/api/v1/populate-database', populate_database,),
    ('/api/v1/get-data-visualisation-data/<field>', get_data_visualisation_data,),
    ('/data-visualisation', data_visualisation,),
]
