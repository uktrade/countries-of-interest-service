datahub_company_primary_key = 'id'
datahub_company_schema = (
    'company_number varchar(12)',
    'id uuid',
    'sector varchar(200)',
)
datahub_company_table_name = 'datahub_company'
omis_primary_key = 'id'
omis_schema = (
    'company_id uuid',
    'country varchar(2)',
    'created_on timestamp',
    'id uuid',
    'sector varchar(200)',
)
omis_table_name = 'omis'
datahub_future_interest_countries_primary_key = 'id'
datahub_future_interest_countries_schema = (
    'company_id uuid',
    'id int',
    'country varchar(2)',
)
datahub_future_interest_countries_table_name = 'datahub_future_interest_countries'
datahub_export_countries_primary_key = 'id'
datahub_export_countries_schema = (
    'company_id uuid',
    'id int',
    'country varchar(2)',
)
datahub_export_countries_table_name = 'datahub_export_countries'
datahub_sector_primary_key = 'sector'
datahub_sector_schema = ('sector varchar(200)',)
datahub_sector_table_name = 'datahub_sector'

countries_and_sectors_of_interest_table_name = 'coi_countries_and_sectors_of_interest'
countries_of_interest_table_name = 'coi_countries_of_interest'
datahub_company_id_to_companies_house_company_number_table_name = \
    'coi_datahub_company_id_to_companies_house_company_number'
export_countries_table_name = 'coi_export_countries'
sectors_of_interest_table_name = 'coi_sectors_of_interest'


