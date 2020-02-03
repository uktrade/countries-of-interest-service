from collections import OrderedDict

from app.config.constants import (
    Source,
    Task,
)
from app.etl.tasks.company_matching import Task as PopulateCompanyMatchingTask
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
from app.etl.tasks.export_wins_country import Task as PopulateExportWinsTask
from app.etl.tasks.interactions_analysed import Task as PopulateAnalysedInteractionsTask
from app.etl.tasks.source_data_extraction import (
    extract_countries_and_territories_reference as ext_countries_and_territories,
    extract_datahub_company_dataset as ext_company,
    extract_datahub_contact_dataset as ext_contact,
    extract_datahub_export_to_countries as ext_export_to_countries,
    extract_datahub_future_interest_countries as ext_future_interest_countries,
    extract_datahub_interactions as ext_interactions,
    extract_datahub_omis as ext_omis,
    extract_export_wins as ext_export_wins,
)


EXTRACTORS_DICT = OrderedDict(
    {
        Source.COUNTRIES_AND_TERRITORIES.value: ext_countries_and_territories,
        Source.DATAHUB_COMPANY.value: ext_company,
        Source.DATAHUB_CONTACT.value: ext_contact,
        Source.DATAHUB_EXPORT_TO_COUNTRIES.value: ext_export_to_countries,
        Source.DATAHUB_INTERACTIONS.value: ext_interactions,
        Source.DATAHUB_FUTURE_INTEREST_COUNTRIES.value: ext_future_interest_countries,
        Source.DATAHUB_OMIS.value: ext_omis,
        Source.EXPORT_WINS.value: ext_export_wins,
    }
)

EXTRACTORS = EXTRACTORS_DICT.keys()


TASKS_DICT = OrderedDict(
    {
        Task.STANDARDISE_COUNTRIES.value: PopulateStandardisedCountriesTask,
        Task.INTERACTIONS_ANALYSED.value: PopulateAnalysedInteractionsTask,
        Task.COUNTRY_SECTOR_INTEREST.value: [
            PopulateDatahubCountryExportedTask,
            PopulateDatahubCountryInterestTask,
            PopulateDatahubOmisCountrySectorInterestTask,
            PopulateExportWinsTask,
            PopulateMentionedInInteractionsTask,
            PopulateCompanyMatchingTask,
        ],
    }
)

TASKS = TASKS_DICT.keys()
