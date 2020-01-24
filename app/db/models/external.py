from sqlalchemy import UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID

from app.db.models import (
    _col,
    _date,
    _dt,
    _int,
    _text,
    BaseModel,
)


class DatahubOmis(BaseModel):

    __tablename__ = 'datahub_omis'
    __table_args__ = {'schema': 'public'}

    id = _col(_int, primary_key=True, autoincrement=True)
    datahub_omis_order_id = _col(UUID(as_uuid=True), unique=True)
    company_id = _col(UUID(as_uuid=True))
    created_date = _col(_dt)
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

    id = _col(_int, primary_key=True, autoincrement=True)
    datahub_company_id = _col(UUID(as_uuid=True), unique=True)
    company_number = _col(_text)
    sector = _col(_text)


class DatahubExportToCountries(BaseModel):

    __tablename__ = 'datahub_export_countries'
    __table_args__ = {'schema': 'public'}

    id = _col(_int, primary_key=True)
    company_id = _col(UUID(as_uuid=True))
    country = _col(_text)
    country_iso_alpha2_code = _col(_text)


class DatahubFutureInterestCountries(BaseModel):

    __tablename__ = 'datahub_future_interest_countries'
    __table_args__ = {'schema': 'public'}

    id = _col(_int, primary_key=True)
    company_id = _col(UUID(as_uuid=True))
    country = _col(_text)
    country_iso_alpha2_code = _col(_text)


class ExportWins(BaseModel):

    __tablename__ = 'export_wins'
    __table_args__ = {'schema': 'public'}

    id = _col(_int, primary_key=True, autoincrement=True)
    export_wins_id = _col(UUID(as_uuid=True), unique=True)
    company_name = _col(_text)
    export_wins_company_id = _col(_text)
    contact_email_address = _col(_text)
    created_on = _col(_dt)
    sector = _col(_text)
    country = _col(_text)
    date_won = _col(_date)


class DITCountryTerritoryRegister(BaseModel):
    __tablename__ = 'dit_country_territory_register'
    __table_args__ = {'schema': 'public'}

    id = _col(_int, primary_key=True, autoincrement=True)
    country_iso_alpha2_code = _col(_text, unique=True)
    end_date = _col(_date)
    name = _col(_text)
    start_date = _col(_date)
    type = _col(_text)


class Interactions(BaseModel):

    __tablename__ = 'interactions'

    id = _col(_int, primary_key=True, autoincrement=True)
    created_on = _col(_dt)
    datahub_company_id = _col(UUID(as_uuid=True))
    datahub_interaction_id = _col(UUID(as_uuid=True))
    notes = _col(_text)
    subject = _col(_text)

    __table_args__ = (
        UniqueConstraint(datahub_interaction_id),
        {'schema': 'public'},
    )
