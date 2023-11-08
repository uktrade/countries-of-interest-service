from app.common.db.models import (
    _bool,
    _col,
    _dt,
    _int,
    _num,
    _sa,
    _text,
    _uuid,
    BaseModel,
)


class CountriesAndSectorsInterest(BaseModel):

    __tablename__ = "countries_and_sectors_interest"

    id = _col(_int, primary_key=True, autoincrement=True)
    service_company_id = _col(_text)
    company_match_id = _col(_int, index=True)
    country = _col(_text)
    sector = _col(_text)
    type = _col(_text, index=True)
    service = _col(_text, index=True)
    source = _col(_text, index=True)
    source_id = _col(_text)
    timestamp = _col(_dt)

    __table_args__ = ({'schema': 'public'},)


class CountriesAndSectorsInterestTemp(BaseModel):

    __tablename__ = "countries_and_sectors_interest_temp"

    id = _col(_int, primary_key=True, autoincrement=True)
    service_company_id = _col(_text)
    company_match_id = _col(_int)
    country = _col(_text)
    sector = _col(_text)
    type = _col(_text)
    service = _col(_text)
    source = _col(_text)
    source_id = _col(_text)
    timestamp = _col(_dt)

    __table_args__ = ({'schema': 'public'},)


class CountriesAndSectorsInterestMatched(BaseModel):

    __tablename__ = "countries_and_sectors_interest_matched"

    id = _col(_int, primary_key=True, autoincrement=True)
    service_company_id = _col(_text)
    company_match_id = _col(_int, index=True)
    country = _col(_text)
    sector = _col(_text)
    type = _col(_text, index=True)
    service = _col(_text, index=True)
    source = _col(_text, index=True)
    source_id = _col(_text)
    timestamp = _col(_dt)

    __table_args__ = ({'schema': 'public'},)


class InteractionsAnalysed(BaseModel):

    __tablename__ = 'interactions_analysed'
    __table_args__ = {'schema': 'algorithm'}

    id = _col(_int, primary_key=True, autoincrement=True)
    datahub_interaction_id = _col(_uuid(as_uuid=True))
    place = _col(_text, nullable=False)
    standardized_place = _col(_text)
    action = _col(_text)
    type = _col(_text)
    context = _col(_sa.ARRAY(_text))
    negation = _col(_bool)


class InteractionsAnalysedInteractionIdLog(BaseModel):

    __tablename__ = 'interactions_analysed_interaction_id_log'
    __table_args__ = {'schema': 'algorithm'}

    datahub_interaction_id = _col(_uuid(as_uuid=True), primary_key=True)
    analysed_at = _col(_dt)


class StandardisedCountries(BaseModel):
    __tablename__ = 'standardised_countries'
    __table_args__ = {'schema': 'algorithm'}

    id = _col(_int, primary_key=True)
    country = _col(_text)
    standardised_country = _col(_text)
    similarity = _col(_num)


class ETLStatus(BaseModel):
    __tablename__ = 'etl_status'
    __table_args__ = {'schema': 'public'}

    id = _col(_int, primary_key=True)
    status = _col(_text)
    timestamp = _col(_dt)


class ETLRuns(BaseModel):
    __tablename__ = 'etl_runs'
    __table_args__ = {'schema': 'public'}

    id = _col(_int, primary_key=True)
    timestamp = _col(_dt)
