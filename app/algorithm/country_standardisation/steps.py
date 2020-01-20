import math
from io import StringIO

from fuzzywuzzy import fuzz, process

from app.db import db_utils
from app.db.db_utils import dsv_buffer_to_table
from app.db.models.external import DITCountryTerritoryRegister
from app.utils import log


@log('Step 1/2 - extract all market interested/exported country sources')
def extract_interested_exported_countries():
    stmt = f"""
    with countries as (
    select distinct country from (
    select trim(country) as country from datahub_export_countries
    union
    select trim(country) as country
        from datahub_future_interest_countries
    union
    select trim(market) as country from datahub_omis
    --TODO: add export wins data post MVP
    --union
    --select trim(country) as country from export_wins
    ) u where country is not null
    )

    select country from countries order by LOWER(country)
    """
    rows = db_utils.execute_query(stmt, df=False)
    countries = [row[0] for row in rows]
    return countries


@log('Step 2/2 - create standardised country table')
def create_standardised_interested_exported_country_table(
    countries, output_schema, output_table
):
    stmt = f"""
    SELECT distinct name FROM {DITCountryTerritoryRegister.__tablename__}
"""
    result = db_utils.execute_query(stmt, df=False)
    choices = [r[0] for r in result]
    lower_choices = [choice.lower() for choice in choices]
    columns = ['id', 'country', 'standardised_country', 'similarity']
    tsv_lines = []
    output_row_count = 1
    for country in countries:
        mappings = _standardise_country(country, choices, lower_choices)
        for standardised_country, similarity in mappings:
            line = [
                str(output_row_count),
                country,
                standardised_country or '',
                str(similarity),
            ]
            tsv_lines.append('\t'.join(line) + '\n')
            output_row_count += 1
    tsv_data = StringIO()
    tsv_data.writelines(tsv_lines)
    tsv_data.seek(0)
    _create_output_table(output_schema, output_table, drop=True)
    dsv_buffer_to_table(tsv_data, output_table, output_schema, columns=columns)


regions = [
    'africa',
    'europe',
    'eu',
    'asia',
    'antarctica',
    'north america',
    'south america',
    'latin america',
    'middle east',
    'far east',
]
replacements = {
    'uae': 'united arab emirates',
    'ksa': 'saudi arabia',
    'usa': 'united states',
    'czech republic': 'czechia',
    'holland': 'the netherlands',
}


def _standardise_country(country, choices, lower_choices):
    lower_value = country.lower()
    # ignore regions
    for region in regions:
        lower_value.replace('south africa', '+')
        lower_value = lower_value.replace(region, '0' * len(region))
        lower_value.replace('+', 'south africa')
    # reformat countries
    for key, value in replacements.items():
        lower_value = lower_value.replace(key, value)
    # check for direct match
    index = lower_choices.index(lower_value) if lower_value in lower_choices else None
    if index:
        return [(choices[index], 100)]
    # check for fuzzy match, include first result and other
    # matches with 90 or higher similarity
    mapped_country = process.extract(
        lower_value.replace('island', '?').replace('central', '?'),
        choices,
        limit=5,
        scorer=fuzz.WRatio,
    )
    top_matches = [mapped_country[0]] + [
        match for match in mapped_country[1:] if match[1] >= 90
    ]

    # adjust score for difference in relative length
    adj_top_matches = [
        (
            match[0],
            max(
                match[1]
                - math.ceil(abs(len(country) - len(match[0])) / (max(len(country), 1)))
                * 4,
                0,
            )
            if match[0].lower().replace('the ', '') not in country.lower()
            else match[1],
        )
        for match in top_matches
    ]

    return adj_top_matches


def _create_output_table(output_schema, output_table, drop=False):
    stmt = f"""
    {f'DROP TABLE IF EXISTS {output_schema}.{output_table};' if drop else ''}
    CREATE TABLE IF NOT EXISTS "{output_schema}"."{output_table}" (
        id INT PRIMARY KEY,
        country TEXT,
        standardised_country TEXT,
        similarity INT
    );
    """
    db_utils.execute_statement(stmt)
