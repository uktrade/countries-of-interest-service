import pytest

from app.db.models import (
    CountriesAndSectorsOfInterest,
    CountriesOfInterest,
    DITCountryTerritoryRegister,
    DatahubCompanyIDToCompaniesHouseCompanyNumber,
    ExportCountries,
    Interactions,
    SectorsOfInterest,
)


@pytest.fixture(scope='module')
def add_companies_house_company_numbers(app_with_db_module):
    def _method(records):
        for record in records:
            defaults = {
                'companies_house_company_number': record.get(
                    'companies_house_company_number', None
                ),
            }
            DatahubCompanyIDToCompaniesHouseCompanyNumber.get_or_create(
                datahub_company_id=record.get('datahub_company_id', None),
                defaults=defaults,
            )

    return _method


@pytest.fixture(scope='module')
def add_countries_and_sectors_of_interest(app_with_db_module):
    def _method(records):
        for record in records:
            defaults = {
                'company_id': record.get('company_id', None),
                'country_of_interest': record.get('country_of_interest', None),
                'sector_of_interest': record.get('sector_of_interest', None),
                'timestamp': record.get('timestamp', None),
            }
            CountriesAndSectorsOfInterest.get_or_create(
                source=record.get('source', None),
                source_id=record.get('source_id', None),
                defaults=defaults,
            )

    return _method


@pytest.fixture(scope='module')
def add_company_countries_of_interest(app_with_db_module):
    def _method(records):
        for record in records:
            defaults = {
                'company_id': record.get('company_id', None),
                'country_of_interest': record.get('country_of_interest', None),
                'timestamp': record.get('timestamp', None),
            }
            CountriesOfInterest.get_or_create(
                source=record.get('source', None),
                source_id=record.get('source_id', None),
                defaults=defaults,
            )

    return _method


@pytest.fixture(scope='module')
def add_company_export_countries(app_with_db_module):
    def _method(records):
        for record in records:
            defaults = {
                'company_id': record.get('company_id', None),
                'export_country': record.get('export_country', None),
                'timestamp': record.get('timestamp', None),
            }
            ExportCountries.get_or_create(
                source=record.get('source', None),
                source_id=record.get('source_id', None),
                defaults=defaults,
            )

    return _method


@pytest.fixture(scope='module')
def add_sectors_of_interest(app_with_db_module):
    def _method(records):
        for record in records:
            defaults = {
                'company_id': record.get('company_id', None),
                'sector_of_interest': record.get('sector_of_interest', None),
                'timestamp': record.get('timestamp', None),
            }
            SectorsOfInterest.get_or_create(
                source=record.get('source', None),
                source_id=record.get('source_id', None),
                defaults=defaults,
            )

    return _method


@pytest.fixture(scope='module')
def add_country_territory_registry(app_with_db_module):
    def _method(records):
        for record in records:
            defaults = {
                'key': record.get('key', None),
                'start_date': record.get('start_date', None),
                'end_date': record.get('end_date', None),
                'name': record.get('name', None),
                'official_name': record.get('official_name', None),
                'type': record.get('type', None),
            }
            DITCountryTerritoryRegister.get_or_create(
                id=record.get('id', None), defaults=defaults,
            )

    return _method


@pytest.fixture(scope='module')
def add_datahub_interaction(app_with_db_module):
    def _method(records):
        for record in records:
            defaults = {
                'datahub_id': record.get('datahub_id', None),
                'companies_house_number': record.get('companies_house_number', None),
                'subject': record.get('subject', None),
                'policy_feedback_notes': record.get('policy_feedback_notes', None),
                'notes': record.get('notes', None),
                'source': record.get('source', None),
                'source_id': record.get('source_id', None),
                'created_on': record.get('created_on', None),
                'country_name': record.get('country_name', None),
                'country_alpha2': record.get('country_alpha2', None),
            }
            Interactions.get_or_create(
                id=record.get('id', None), defaults=defaults,
            )

    return _method
