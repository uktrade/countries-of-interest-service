from collections import OrderedDict

from app.config.constants import Task as TaskConstant
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
    ExtractCountriesAndTerritoriesReferenceDataset,
    ExtractDatahubCompanyDataset,
    ExtractDatahubContactDataset,
    ExtractDatahubExportToCountries,
    ExtractDatahubFutureInterestCountries,
    ExtractDatahubInteractions,
    ExtractDatahubOmis,
    ExtractExportWins,
)


EXTRACTORS_LIST = [
    ExtractCountriesAndTerritoriesReferenceDataset,
    ExtractDatahubCompanyDataset,
    ExtractDatahubContactDataset,
    ExtractDatahubExportToCountries,
    ExtractDatahubInteractions,
    ExtractDatahubFutureInterestCountries,
    ExtractDatahubOmis,
    ExtractExportWins,
]

EXTRACTORS = [extractor.name for extractor in EXTRACTORS_LIST]


TASKS_DICT = OrderedDict(
    {
        TaskConstant.STANDARDISE_COUNTRIES.value: PopulateStandardisedCountriesTask,
        TaskConstant.INTERACTIONS_ANALYSED.value: PopulateAnalysedInteractionsTask,
        TaskConstant.COUNTRY_SECTOR_INTEREST.value: [
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
