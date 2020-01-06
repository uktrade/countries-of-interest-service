from flask_sqlalchemy import SQLAlchemy

from sqlalchemy import PrimaryKeyConstraint, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import ClauseElement


db = SQLAlchemy()
sql_alchemy = db

# aliases
_sa = sql_alchemy
_col = db.Column
_text = db.Text
_int = db.Integer
_dt = db.DateTime
_bigint = _sa.BigInteger
_bool = db.Boolean
_num = db.Numeric
_array = _sa.ARRAY
_date = _sa.Date


class BaseModel(db.Model):
    __abstract__ = True

    def save(self):
        _sa.session.add(self)
        _sa.session.commit()

    @classmethod
    def get_or_create(cls, defaults=None, **kwargs):
        """
        Creates a new object or returns existing.

        Example:
            object, created = Model.get_or_create(a=1, b=2, defaults=dict(c=3))

        :param defaults: (dictionary) of fields that should be saved on new instance
        :param kwargs: fields to query for an object
        :return: (Object, boolean) (Object, created)
        """
        instance = _sa.session.query(cls).filter_by(**kwargs).first()
        if instance:
            return instance, False
        else:
            params = dict(
                (k, v) for k, v in kwargs.items() if not isinstance(v, ClauseElement)
            )
            params.update(defaults or {})
            instance = cls(**params)
            instance.save()
            return instance, True


class HawkUsers(BaseModel):

    __tablename__ = 'hawk_users'
    __table_args__ = {'schema': 'public'}

    id = _col(_text, primary_key=True)
    key = _col(_text)
    scope = _col(_array(_text))
    description = _col(_text)

    @classmethod
    def get_client_key(cls, client_id):
        query = sql_alchemy.session.query(cls.key).filter(cls.id == client_id)
        result = query.first()
        return result[0] if result else None

    @classmethod
    def get_client_scope(cls, client_id):
        query = sql_alchemy.session.query(cls.scope).filter(cls.id == client_id)
        result = query.first()
        return result[0] if result else None

    @classmethod
    def add_user(cls, client_id, client_key, client_scope, description):
        cls.get_or_create(
            id=client_id,
            defaults={
                'key': client_key,
                'scope': client_scope,
                'description': description,
            },
        )


class CountriesAndSectorsOfInterest(BaseModel):

    __tablename__ = 'coi_countries_and_sectors_of_interest'

    company_id = _col(_text)
    country_of_interest = _col(_text)
    standardised_country = _col(_text)
    sector_of_interest = _col(_text)
    source = _col(_text)
    source_id = _col(_text)
    timestamp = _col(_dt)

    __table_args__ = (
        PrimaryKeyConstraint(source, source_id),
        {'schema': 'public'},
    )


class CountriesOfInterest(BaseModel):

    __tablename__ = 'coi_countries_of_interest'

    company_id = _col(_text)
    country_of_interest = _col(_text)
    standardised_country = _col(_text)
    source = _col(_text)
    source_id = _col(_text)
    timestamp = _col(_dt)

    __table_args__ = (
        PrimaryKeyConstraint(source, source_id),
        {'schema': 'public'},
    )


class DatahubOmis(BaseModel):

    __tablename__ = 'datahub_omis'
    __table_args__ = {'schema': 'public'}

    company_id = _col(UUID(as_uuid=True))
    created_date = _col(_dt)
    id = _col(UUID(as_uuid=True), primary_key=True)
    market = _col(_text)
    sector = _col(_text)


class DatahubSectors(BaseModel):

    __tablename__ = 'datahub_sectors'
    __table_args__ = {'schema': 'public'}

    id = _col(UUID(as_uuid=True), primary_key=True)
    sector = _col(_text)


class DatahubCompany(BaseModel):

    __tablename__ = 'datahub_company'
    __table_args__ = {'schema': 'public'}

    id = _col(UUID(as_uuid=True), primary_key=True)
    company_number = _col(_text)
    sector = _col(_text)


class DatahubExportToCountries(BaseModel):

    __tablename__ = 'datahub_export_countries'
    __table_args__ = {'schema': 'public'}

    company_id = _col(UUID(as_uuid=True))
    country = _col(_text)
    country_iso_alpha2_code = _col(_text)
    id = _col(_num, primary_key=True)


class DatahubFutureInterestCountries(BaseModel):

    __tablename__ = 'datahub_future_interest_countries'
    __table_args__ = {'schema': 'public'}

    company_id = _col(UUID(as_uuid=True))
    country = _col(_text)
    country_iso_alpha2_code = _col(_text)
    id = _col(_num, primary_key=True)


class ExportCountries(BaseModel):

    __tablename__ = 'coi_export_countries'

    company_id = _col(_text)
    export_country = _col(_text)
    standardised_country = _col(_text)
    source = _col(_text)
    source_id = _col(_text)
    timestamp = _col(_dt)

    __table_args__ = (
        PrimaryKeyConstraint(source, source_id),
        {'schema': 'public'},
    )


class ExportWins(BaseModel):

    __tablename__ = 'export_wins'
    __table_args__ = {'schema': 'public'}

    company_id = _col(_text)
    country = _col(_text)
    id = _col(UUID(as_uuid=True), primary_key=True)
    timestamp = _col(_dt)


class SectorsOfInterest(BaseModel):

    __tablename__ = 'coi_sectors_of_interest'

    company_id = _col(_text)
    sector_of_interest = _col(_text)
    source = _col(_text)
    source_id = _col(_text)
    timestamp = _col(_dt)

    __table_args__ = (
        PrimaryKeyConstraint(source, source_id),
        {'schema': 'public'},
    )


class DITCountryTerritoryRegister(BaseModel):
    __tablename__ = 'dit_country_territory_register'
    __table_args__ = {'schema': 'public'}

    end_date = _col(_date)
    id = _col(_text, primary_key=True)
    name = _col(_text)
    start_date = _col(_date)
    type = _col(_text)


class StandardisedCountries(BaseModel):
    __tablename__ = 'standardised_countries'
    __table_args__ = {'schema': 'public'}

    id = _col(_int, primary_key=True)
    country = _col(_text)
    standardised_country = _col(_text)
    similarity = _col(_num)


class Interactions(BaseModel):

    __tablename__ = 'interactions'

    id = _col(_int, primary_key=True, autoincrement=True)
    datahub_company_id = _col(UUID(as_uuid=True))
    datahub_interaction_id = _col(UUID(as_uuid=True))
    subject = _col(_text)
    notes = _col(_text)
    created_on = _col(_dt)

    __table_args__ = (
        UniqueConstraint(datahub_interaction_id),
        {'schema': 'public'},
    )


class InteractionsAnalysed(BaseModel):

    __tablename__ = 'interactions_analysed'
    __table_args__ = {'schema': 'algorithm'}

    id = _col(_int, primary_key=True)
    place = _col(_text, nullable=False)
    standardized_place = _col(_text)
    action = _col(_text)
    type = _col(_text)
    context = _col(_sa.ARRAY(_text))
    negation = _col(_bool)
