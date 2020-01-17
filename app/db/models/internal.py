from sqlalchemy import PrimaryKeyConstraint
from sqlalchemy.dialects.postgresql import UUID

from app.db.models import (
    _array,
    _bool,
    _col,
    _dt,
    _int,
    _num,
    _sa,
    _text,
    BaseModel,
)


class HawkUsers(BaseModel):

    __tablename__ = 'hawk_users'
    __table_args__ = {'schema': 'public'}

    id = _col(_text, primary_key=True)
    key = _col(_text)
    scope = _col(_array(_text))
    description = _col(_text)

    @classmethod
    def get_client_key(cls, client_id):
        query = _sa.session.query(cls.key).filter(cls.id == client_id)
        result = query.first()
        return result[0] if result else None

    @classmethod
    def get_client_scope(cls, client_id):
        query = _sa.session.query(cls.scope).filter(cls.id == client_id)
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


class MentionedInInteractions(BaseModel):

    __tablename__ = "coi_mentioned_in_interactions"

    id = _col(_int, primary_key=True, autoincrement=True)
    company_id = _col(_text)
    country_of_interest = _col(_text)
    interaction_id = _col(_text)
    timestamp = _col(_dt)

    __table_args__ = ({'schema': 'public'},)


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


class ExportCountries(BaseModel):

    __tablename__ = 'coi_export_countries'

    company_id = _col(_text)
    export_country = _col(_text)
    source = _col(_text)
    source_id = _col(_text)
    standardised_country = _col(_text)
    timestamp = _col(_dt)

    __table_args__ = (
        PrimaryKeyConstraint(source, source_id),
        {'schema': 'public'},
    )


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


class InteractionsAnalysed(BaseModel):

    __tablename__ = 'interactions_analysed'
    __table_args__ = {'schema': 'algorithm'}

    id = _col(_int, primary_key=True, autoincrement=True)
    datahub_interaction_id = _col(UUID(as_uuid=True))
    place = _col(_text, nullable=False)
    standardized_place = _col(_text)
    action = _col(_text)
    type = _col(_text)
    context = _col(_sa.ARRAY(_text))
    negation = _col(_bool)


class InteractionsAnalysedInteractionIdLog(BaseModel):

    __tablename__ = 'interactions_analysed_interaction_id_log'
    __table_args__ = {'schema': 'algorithm'}

    datahub_interaction_id = _col(UUID(as_uuid=True), primary_key=True)
    analysed_at = _col(_dt)


class StandardisedCountries(BaseModel):
    __tablename__ = 'standardised_countries'
    __table_args__ = {'schema': 'public'}

    id = _col(_int, primary_key=True)
    country = _col(_text)
    standardised_country = _col(_text)
    similarity = _col(_num)
