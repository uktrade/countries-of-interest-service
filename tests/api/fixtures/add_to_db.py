import pytest

from app.db.models import (
    CountriesAndSectorsOfInterest,
    CountriesOfInterest,
    DatahubCompanyIDToCompaniesHouseCompanyNumber,
    ExportCountries,
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
