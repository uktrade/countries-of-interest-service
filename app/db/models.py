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


class DatahubCompanyIDToCompaniesHouseCompanyNumber(BaseModel):

    __tablename__ = 'coi_datahub_company_id_to_companies_house_company_number'
    __table_args__ = {'schema': 'public'}

    datahub_company_id = _col(UUID(as_uuid=True), primary_key=True)
    companies_house_company_number = _col(_text)


class CountriesAndSectorsOfInterest(BaseModel):

    __tablename__ = 'coi_countries_and_sectors_of_interest'
    __table_args__ = {'schema': 'public'}

    company_id = _col(_text)
    country_of_interest = _col(_text)
    sector_of_interest = _col(_text)
    source = _col(_text)
    source_id = _col(_text)
    timestamp = _col(_dt)

    __table_args__ = (PrimaryKeyConstraint(source, source_id),)


class CountriesOfInterest(BaseModel):

    __tablename__ = 'coi_countries_of_interest'
    __table_args__ = {'schema': 'public'}

    company_id = _col(_text)
    country_of_interest = _col(_text)
    source = _col(_text)
    source_id = _col(_text)
    timestamp = _col(_dt)

    __table_args__ = (PrimaryKeyConstraint(source, source_id),)


class ExportCountries(BaseModel):

    __tablename__ = 'coi_export_countries'
    __table_args__ = {'schema': 'public'}

    company_id = _col(_text)
    export_country = _col(_text)
    source = _col(_text)
    source_id = _col(_text)
    timestamp = _col(_dt)

    __table_args__ = (PrimaryKeyConstraint(source, source_id),)


class SectorsOfInterest(BaseModel):

    __tablename__ = 'coi_sectors_of_interest'
    __table_args__ = {'schema': 'public'}

    company_id = _col(_text)
    sector_of_interest = _col(_text)
    source = _col(_text)
    source_id = _col(_text)
    timestamp = _col(_dt)

    __table_args__ = (PrimaryKeyConstraint(source, source_id),)


class DITCountryTerritoryRegister(BaseModel):
    __tablename__ = 'dit_country_territory_register'
    __table_args__ = {'schema': 'public'}

    id = _col(_int, primary_key=True)
    key = _col(_text)
    start_date = _col(_date)
    end_date = _col(_date)
    name = _col(_text)
    official_name = _col(_text)
    type = _col(_text)


class Interactions(BaseModel):

    __tablename__ = 'interactions'

    id = _col(_int, primary_key=True, autoincrement=True)
    datahub_id = _col(_text)
    companies_house_number = _col(_text)
    subject = _col(_text)
    policy_feedback_notes = _col(_text)
    notes = _col(_text)
    source = _col(_text)
    source_id = _col(_text)
    created_on = _col(_dt)
    country_name = _col(_text)
    country_alpha2 = _col(_text)

    __table_args__ = (
        UniqueConstraint(source, source_id),
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
