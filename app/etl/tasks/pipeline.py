from collections import OrderedDict

from app.etl.tasks.country_standardisation import PopulateStandardisedCountriesTask
from app.etl.tasks.datahub_country_export import (
    Task as PopulateDatahubCountryExportedTask,
)
from app.etl.tasks.datahub_country_interest import (
    Task as PopulateDatahubCountryInterestTask,
)
from app.etl.tasks.datahub_interaction_country import (
    Task as PopulateMentionedInInteractionsTask,
)
from app.etl.tasks.datahub_omis_country_sector_interest import (
    Task as PopulateDatahubOmisCountrySectorInterestTask,
)
from app.etl.tasks.interactions_analysed import Task as PopulateAnalysedInteractionsTask
from app.etl.tasks.source_data_extraction import (
    extract_countries_and_territories_reference_dataset,
    extract_datahub_company_dataset,
    extract_datahub_export_to_countries,
    extract_datahub_future_interest_countries,
    extract_datahub_interactions,
    extract_datahub_omis,
    extract_export_wins,
)

COUNTRIES_AND_TERRITORIES = 'countries_and_territories'
DATAHUB_COMPANY = 'datahub_company'
DATAHUB_EXPORT_TO_COUNTRIES = 'datahub_export_to_countries'
DATAHUB_INTERACTIONS = 'datahub_interactions'
DATAHUB_FUTURE_INTEREST_COUNTRIES = 'datahub_future_interest_countries'
DATAHUB_OMIS = 'datahub_omis'
EXPORT_WINS = 'export_wins'


STANDARDISE_COUNTRIES = 'standardise_countries'
INTERACTIONS_ANALYSED = 'interactions_analysed'
COUNTRY_SECTOR_INTEREST = 'country_sector_interest'

EXTRACTORS_DICT = OrderedDict(
    {
        COUNTRIES_AND_TERRITORIES: extract_countries_and_territories_reference_dataset,
        DATAHUB_COMPANY: extract_datahub_company_dataset,
        DATAHUB_EXPORT_TO_COUNTRIES: extract_datahub_export_to_countries,
        DATAHUB_INTERACTIONS: extract_datahub_interactions,
        DATAHUB_FUTURE_INTEREST_COUNTRIES: extract_datahub_future_interest_countries,
        DATAHUB_OMIS: extract_datahub_omis,
        EXPORT_WINS: extract_export_wins,
    }
)

EXTRACTORS = EXTRACTORS_DICT.keys()


TASKS_DICT = OrderedDict(
    {
        STANDARDISE_COUNTRIES: PopulateStandardisedCountriesTask,
        INTERACTIONS_ANALYSED: PopulateAnalysedInteractionsTask,
        COUNTRY_SECTOR_INTEREST: [
            PopulateDatahubCountryExportedTask,
            PopulateDatahubCountryInterestTask,
            PopulateDatahubOmisCountrySectorInterestTask,
            PopulateMentionedInInteractionsTask,
        ],
    }
)

TASKS = TASKS_DICT.keys()
