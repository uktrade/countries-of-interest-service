from collections import OrderedDict

from app.etl.tasks.countries_and_sectors_of_interest import (
    Task as PopulateCountriesAndSectorsOfInterestTask,
)
from app.etl.tasks.countries_of_interest import Task as PopulateCountriesOfInterestTask
from app.etl.tasks.country_standardisation import PopulateStandardisedCountriesTask
from app.etl.tasks.export_countries import Task as ExportToCountriesTask
from app.etl.tasks.interactions_analysed import Task as PopulateAnalysedInteractionsTask
from app.etl.tasks.mentioned_in_interactions import (
    Task as PopulateMentionedInInteractionsTask,
)
from app.etl.tasks.sectors_of_interest import Task as SectorsOfInterestTask
from app.etl.tasks.source_data_extraction import (
    extract_countries_and_territories_reference_dataset,
    extract_datahub_company_dataset,
    extract_datahub_export_to_countries,
    extract_datahub_future_interest_countries,
    extract_datahub_interactions,
    extract_datahub_omis,
)


COUNTRIES_AND_TERRITORIES = 'countries_and_territories'
DATAHUB_COMPANY = 'datahub_company'
DATAHUB_EXPORT_TO_COUNTRIES = 'datahub_export_to_countries'
DATAHUB_INTERACTIONS = 'datahub_interactions'
DATAHUB_FUTURE_INTEREST_COUNTRIES = 'datahub_future_interest_countries'
DATAHUB_OMIS = 'datahub_omis'


STANDARDISE_COUNTRIES = 'standardise_countries'
INTERACTIONS_ANALYSED = 'interactions_analysed'
EXPORT_TO_COUNTRIES = 'export_to_countries'
COUNTRIES_AND_SECTORS_OF_INTEREST = 'countries_and_sectors_of_interest'
COUNTRIES_OF_INTEREST = 'countries_of_interest'
SECTORS_OF_INTEREST = 'sectors_of_interest'
MENTIONED_IN_INTERACTIONS = 'mentioned_in_interactions'

EXTRACTORS_DICT = OrderedDict(
    {
        COUNTRIES_AND_TERRITORIES: extract_countries_and_territories_reference_dataset,
        DATAHUB_COMPANY: extract_datahub_company_dataset,
        DATAHUB_EXPORT_TO_COUNTRIES: extract_datahub_export_to_countries,
        DATAHUB_INTERACTIONS: extract_datahub_interactions,
        DATAHUB_FUTURE_INTEREST_COUNTRIES: extract_datahub_future_interest_countries,
        DATAHUB_OMIS: extract_datahub_omis,
    }
)

EXTRACTORS = EXTRACTORS_DICT.keys()


TASKS_DICT = OrderedDict(
    {
        STANDARDISE_COUNTRIES: PopulateStandardisedCountriesTask,
        INTERACTIONS_ANALYSED: PopulateAnalysedInteractionsTask,
        EXPORT_TO_COUNTRIES: ExportToCountriesTask,
        COUNTRIES_AND_SECTORS_OF_INTEREST: PopulateCountriesAndSectorsOfInterestTask,
        COUNTRIES_OF_INTEREST: PopulateCountriesOfInterestTask,
        SECTORS_OF_INTEREST: SectorsOfInterestTask,
        MENTIONED_IN_INTERACTIONS: PopulateMentionedInInteractionsTask,
    }
)

TASKS = TASKS_DICT.keys()
