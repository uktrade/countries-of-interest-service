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
    DATAHUB_CONTACT = 'datahub_contact'
    DATAHUB_COMPANY = 'datahub_company'
    DATAHUB_EXPORT_TO_COUNTRIES = 'datahub_export_countries'
    DATAHUB_INTERACTIONS = 'datahub_interactions'
    DATAHUB_FUTURE_INTEREST_COUNTRIES = 'datahub_future_interest_countries'
    DATAHUB_OMIS = 'datahub_omis'
    EXPORT_WINS = 'export_wins'


class Task(BaseEnum):
    STANDARDISE_COUNTRIES = 'standardise_countries'
    INTERACTIONS_ANALYSED = 'interactions_analysed'
    COUNTRY_SECTOR_INTEREST = 'country_sector_interest'


class Service(BaseEnum):
    DATAHUB = 'datahub'
    EXPORT_WINS = 'export_wins'


class Type(BaseEnum):
    EXPORTED = 'exported'
    MENTIONED = 'mentioned'
    INTERESTED = 'interested'
