from enum import Enum


class BaseEnum(Enum):
    @classmethod
    def has_value(cls, value):
        return value in cls._value2member_map_

    @classmethod
    def values(cls):
        return list(cls._value2member_map_.keys())


class Source(BaseEnum):
    COUNTRIES_AND_TERRITORIES = 'countries_and_territories'
    DATAHUB_CONTACT = 'contact'
    DATAHUB_COMPANY = 'company'
    DATAHUB_COMPANY_EXPORT_COUNTRY = 'company_export_country'
    DATAHUB_EXPORT_COUNTRY_HISTORY = 'export_country_history'
    DATAHUB_EXPORT_TO_COUNTRIES = 'export_countries'
    DATAHUB_INTERACTIONS = 'interactions'
    DATAHUB_INTERACTIONS_EXPORT_COUNTRY = 'interactions_export_country'
    DATAHUB_FUTURE_INTEREST_COUNTRIES = 'future_interest_countries'
    DATAHUB_OMIS = 'omis'
    EXPORT_WINS = 'export_wins'


class Task(BaseEnum):
    INTERACTIONS_EXPORT_COUNTRY = 'interactions_export_country'
    STANDARDISE_COUNTRIES = 'standardise_countries'
    INTERACTIONS_ANALYSED = 'interactions_analysed'
    COUNTRY_SECTOR_INTEREST = 'country_sector_interest'
    COMPANY_MATCHING = 'company_matching'
    EXPORT_COUNTRIES = 'export_countries'
    COUNTRIES_OF_INTEREST = 'countries_of_interest'
    MENTIONED_IN_INTERACTIONS = 'mentioned_in_interactions'
    COUNTRIES_AND_SECTORS_OF_INTEREST = 'countries_and_sectors_of_interest'
    EXPORT_WINS = 'export_wins'


class Service(BaseEnum):
    DATAHUB = 'datahub'
    EXPORT_WINS = 'export_wins'


class Type(BaseEnum):
    EXPORTED = 'exported'
    MENTIONED = 'mentioned'
    INTERESTED = 'interested'
