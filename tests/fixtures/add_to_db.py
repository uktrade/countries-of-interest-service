import pytest

from app.db.models.external import (
    DatahubExportToCountries,
    DatahubFutureInterestCountries,
    DatahubOmis,
    DatahubSectors,
    DITCountryTerritoryRegister,
    ExportWins,
    Interactions,
)
from app.db.models.internal import (
    CountriesAndSectorsOfInterest,
    CountriesOfInterest,
    ExportCountries,
    InteractionsAnalysed,
    InteractionsAnalysedInteractionIdLog,
    MentionedInInteractions,
    SectorsOfInterest,
    StandardisedCountries,
)


@pytest.fixture(scope='module')
def add_countries_and_sectors_of_interest(app_with_db_module):
    def _method(records):
        for record in records:
            defaults = {
                'company_id': record.get('company_id', None),
                'country_of_interest': record.get('country_of_interest', None),
                'standardised_country': record.get('standardised_country', None),
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
                'standardised_country': record.get('standardised_country', None),
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
                'standardised_country': record.get('standardised_country', None),
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
def add_datahub_export_to_countries(app_with_db_module):
    def _method(records):
        for record in records:
            defaults = {
                'company_id': record.get('company_id', None),
                'country': record.get('country', None),
                'country_iso_alpha2_code': record.get('country_iso_alpha2_code', None),
                'id': record.get('id', None),
            }
            DatahubExportToCountries.get_or_create(
                id=record.get('id', None), defaults=defaults
            )

    return _method


@pytest.fixture(scope='module')
def add_datahub_future_interest_countries(app_with_db_module):
    def _method(records):
        for record in records:
            defaults = {
                'company_id': record.get('company_id', None),
                'country': record.get('country', None),
                'country_iso_alpha2_code': record.get('country_iso_alpha2_code', None),
                'id': record.get('id', None),
            }
            DatahubFutureInterestCountries.get_or_create(
                id=record.get('id', None), defaults=defaults
            )

    return _method


@pytest.fixture(scope='module')
def add_datahub_omis(app_with_db_module):
    def _method(records):
        for record in records:
            defaults = {
                'company_id': record.get('company_id', None),
                'created_date': record.get('created_date', None),
                'market': record.get('market', None),
                'sector': record.get('sector', None),
                'datahub_omis_order_id': record.get('datahub_omis_order_id', None),
            }
            DatahubOmis.get_or_create(
                datahub_omis_order_id=record.get('datahub_omis_order_id', None),
                defaults=defaults,
            )

    return _method


@pytest.fixture(scope='module')
def add_datahub_sectors(app_with_db_module):
    def _method(records):
        for record in records:
            defaults = {
                'id': record.get('id', None),
                'sector': record.get('sector', None),
            }
            DatahubSectors.get_or_create(id=record.get('id', None), defaults=defaults)

    return _method


@pytest.fixture(scope='module')
def add_country_territory_registry(app_with_db_module):
    def _method(records):
        for record in records:
            defaults = {
                'end_date': record.get('end_date', None),
                'name': record.get('name', None),
                'start_date': record.get('start_date', None),
                'type': record.get('type', None),
                'country_iso_alpha2_code': record.get('country_iso_alpha2_code', None),
            }
            DITCountryTerritoryRegister.get_or_create(
                country_iso_alpha2_code=record.get('country_iso_alpha2_code', None),
                defaults=defaults,
            )

    return _method


@pytest.fixture(scope='module')
def add_export_wins(app_with_db_module):
    def _method(records):
        for record in records:
            defaults = {
                'company_id': record.get('company_id', None),
                'country': record.get('country', None),
                'id': record.get('id', None),
                'timestamp': record.get('timestamp', None),
            }
            ExportWins.get_or_create(id=record.get('id', None), defaults=defaults)

    return _method


@pytest.fixture(scope='module')
def add_mentioned_in_interactions(app_with_db_module):
    def _method(records):
        for record in records:
            defaults = {
                'company_id': record.get('company_id', None),
                'country_of_interest': record.get('country_of_interest', None),
                'source': record.get('source', None),
                'source_id': record[('source_id', None)],
                'timestamp': record.get('timestamp', None),
            }
            MentionedInInteractions.get_or_create(
                id=record.get('id', None), defaults=defaults
            )

    return _method


@pytest.fixture(scope='module')
def add_standardised_countries(app_with_db_module):
    def _method(records):
        for record in records:
            defaults = {
                'id': record.get('id', None),
                'country': record.get('country', None),
                'standardised_country': record.get('standardised_country', None),
                'similarity': record.get('similarity', None),
            }
            StandardisedCountries.get_or_create(
                id=record.get('id', None), defaults=defaults
            )

    return _method


@pytest.fixture(scope='module')
def add_datahub_interaction(app_with_db_module):
    def _method(records):
        for record in records:
            defaults = {
                'datahub_interaction_id': record.get('datahub_interaction_id', None),
                'datahub_company_id': record.get('datahub_company_id', None),
                'subject': record.get('subject', None),
                'notes': record.get('notes', None),
                'created_on': record.get('created_on', None),
            }
            Interactions.get_or_create(
                id=record.get('id', None), defaults=defaults,
            )

    return _method


@pytest.fixture(scope='module')
def add_interactions_analysed(app_with_db_module):
    def _method(records):
        for record in records:
            defaults = {
                'datahub_interaction_id': record.get('datahub_interaction_id', None),
                'place': record.get('place', None),
                'standardized_place': record.get('standardized_place', None),
                'action': record.get('action', None),
                'type': record.get('type', None),
                'context': record.get('context', None),
                'negation': record.get('negation', None),
            }
            InteractionsAnalysed.get_or_create(
                id=record.get('id', None), defaults=defaults,
            )

    return _method


@pytest.fixture(scope='module')
def add_interactions_analysed_interaction_id_log(app_with_db_module):
    def _method(records):
        for record in records:
            defaults = {
                'datahub_interaction_id': record.get('datahub_interaction_id', None),
            }
            InteractionsAnalysedInteractionIdLog.get_or_create(defaults=defaults)

    return _method
