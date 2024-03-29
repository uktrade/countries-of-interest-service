from enum import Enum

from app.common.db.models import (
    _col,
    _date,
    _dt,
    _int,
    _text,
    _unique,
    _uuid,
    BaseModel,
)


class DatahubOmis(BaseModel):
    __tablename__ = 'datahub_omis'
    __table_args__ = {'schema': 'public'}

    id = _col(_int, primary_key=True, autoincrement=True)
    datahub_omis_order_id = _col(_uuid(as_uuid=True), unique=True)
    company_id = _col(_uuid(as_uuid=True))
    created_date = _col(_dt)
    market = _col(_text)
    sector = _col(_text)


class DatahubCompany(BaseModel):
    __tablename__ = 'datahub_company'
    __table_args__ = {'schema': 'public'}

    id = _col(_int, primary_key=True, autoincrement=True)
    company_name = _col(_text)
    datahub_company_id = _col(_uuid(as_uuid=True), unique=True)
    companies_house_id = _col(_text)
    sector = _col(_text)
    reference_code = _col(_text)
    postcode = _col(_text)
    modified_on = _col(_dt)


class DatahubCompanyExportCountry(BaseModel):
    __tablename__ = 'datahub_company_export_country'
    __table_args__ = {'schema': 'public'}

    id = _col(_int, primary_key=True, autoincrement=True)
    company_export_country_id = _col(_uuid(as_uuid=True))
    company_id = _col(_uuid(as_uuid=True))
    country = _col(_text)
    country_iso_alpha2_code = _col(_text)
    created_on = _col(_dt)
    modified_on = _col(_dt)
    status = _col(_text)


class DatahubCompanyExportCountryHistory(BaseModel):
    __tablename__ = 'datahub_export_country_history'
    __table_args__ = {'schema': 'public'}

    class Status(Enum):
        CURRENTLY_EXPORTING = 'currently_exporting'
        FUTURE_INTEREST = 'future_interest'

    id = _col(_int, primary_key=True, autoincrement=True)
    company_id = _col(_uuid(as_uuid=True))
    country = _col(_text)
    country_iso_alpha2_code = _col(_text)
    history_date = _col(_dt)
    history_id = _col(_uuid(as_uuid=True), unique=True)
    history_type = _col(_text)
    status = _col(_text)


class DatahubContact(BaseModel):
    __tablename__ = 'datahub_contact'
    __table_args__ = {'schema': 'public'}

    id = _col(_int, primary_key=True, autoincrement=True)
    datahub_contact_id = _col(_uuid(as_uuid=True), unique=True)
    datahub_company_id = _col(_uuid(as_uuid=True))
    email = _col(_text)


class ExportWins(BaseModel):
    __tablename__ = 'export_wins'
    __table_args__ = {'schema': 'public'}

    id = _col(_int, primary_key=True, autoincrement=True)
    export_wins_id = _col(_uuid(as_uuid=True), unique=True)
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
    datahub_company_id = _col(_uuid(as_uuid=True))
    datahub_interaction_id = _col(_uuid(as_uuid=True))
    notes = _col(_text)
    subject = _col(_text)

    __table_args__ = (
        _unique(datahub_interaction_id),
        {'schema': 'public'},
    )


class InteractionsExportCountry(BaseModel):
    __tablename__ = 'interactions_export_country'

    id = _col(_int, primary_key=True, autoincrement=True)
    country_iso_alpha2_code = _col(_text)
    country_name = _col(_text)
    created_on = _col(_dt)
    datahub_company_id = _col(_uuid(as_uuid=True))
    datahub_interaction_export_country_id = _col(_uuid(as_uuid=True), unique=True)
    datahub_interaction_id = _col(_uuid(as_uuid=True))
    status = _col(_text)

    __table_args__ = ({'schema': 'public'},)
