from collections import OrderedDict

from app.config.constants import Task as TaskConstant
from app.etl.tasks.company_matching import Task as PopulateCompanyMatchingTask
from app.etl.tasks.country_standardisation import PopulateStandardisedCountriesTask
from app.etl.tasks.datahub_company_export_country import (
    Task as PopulateDatahubCompanyExportCountryTask,
)
from app.etl.tasks.datahub_country_export import Task as PopulateDatahubCountryExportedTask
from app.etl.tasks.datahub_country_interest import Task as PopulateDatahubCountryInterestTask
from app.etl.tasks.datahub_interaction_country import Task as PopulateMentionedInInteractionsTask
from app.etl.tasks.datahub_interactions_export_country import (
    Task as PopulateDatahubInteractionsExportCountryTask,
)
from app.etl.tasks.datahub_omis_country_sector_interest import (
    Task as PopulateDatahubOmisCountrySectorInterestTask,
)
from app.etl.tasks.export_wins_country import Task as PopulateExportWinsTask
from app.etl.tasks.interactions_analysed import Task as PopulateAnalysedInteractionsTask
from app.etl.tasks.source_data_extraction import (
    ExtractCountriesAndTerritoriesReferenceDataset,
    ExtractDatahubCompanyDataset,
    ExtractDatahubCompanyExportCountry,
    ExtractDatahubContactDataset,
    ExtractDatahubExportCountryHistory,
    ExtractDatahubExportToCountries,
    ExtractDatahubFutureInterestCountries,
    ExtractDatahubInteractions,
    ExtractDatahubInteractionsExportCountry,
    ExtractDatahubOmis,
    ExtractExportWins,
)


EXTRACTORS_LIST = [
    ExtractCountriesAndTerritoriesReferenceDataset,
    ExtractDatahubCompanyDataset,
    ExtractDatahubCompanyExportCountry,
    ExtractDatahubContactDataset,
    ExtractDatahubExportCountryHistory,
    ExtractDatahubExportToCountries,
    ExtractDatahubInteractions,
    ExtractDatahubInteractionsExportCountry,
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
            PopulateDatahubCompanyExportCountryTask,
            PopulateDatahubCountryExportedTask,
            PopulateDatahubCountryInterestTask,
            PopulateDatahubOmisCountrySectorInterestTask,
            PopulateExportWinsTask,
            PopulateMentionedInInteractionsTask,
            PopulateDatahubInteractionsExportCountryTask,
            PopulateCompanyMatchingTask,
        ],
    }
)

TASKS = TASKS_DICT.keys()
