import math
from io import StringIO

from flask import current_app as flask_app
from fuzzywuzzy import fuzz, process

from app.db.models.external import (
    DatahubCompanyExportCountry,
    DatahubCompanyExportCountryHistory,
    DatahubOmis,
    DITCountryTerritoryRegister,
    ExportWins,
)
from app.utils import log


@log.write('Step 1/2 - extract all market interested/exported country sources')
def extract_interested_exported_countries():
    stmt = f"""
    with countries as (
        select distinct country from (
            select trim(country) as country
            from {DatahubCompanyExportCountry.get_fq_table_name()}
            union
            select trim(country) as country
            from {DatahubCompanyExportCountryHistory.get_fq_table_name()}
            union
            select trim(market) as country
            from {DatahubOmis.get_fq_table_name()}
            union
            select trim(country) as country
            from {ExportWins.get_fq_table_name()}
        ) u where country is not null
    )
    select country from countries order by LOWER(country)
    """
    rows = flask_app.dbi.execute_query(stmt, df=False)
    countries = [row[0] for row in rows]
    return countries


@log.write('Step 2/2 - create standardised country table')
def create_standardised_interested_exported_country_table(countries, output_schema, output_table):
    stmt = f"""
    SELECT distinct name FROM {DITCountryTerritoryRegister.__tablename__}
"""
    result = flask_app.dbi.execute_query(stmt, df=False)
    choices = [r[0] for r in result] + list(split_mappings.keys())
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
    flask_app.dbi.dsv_buffer_to_table(tsv_data, f'{output_schema}.{output_table}', columns)


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
    'bahamas': 'the bahamas',
    'cabo verde': 'cape verde',
    'congo (the democratic republic of the)': 'congo',
    'czech republic': 'czechia',
    "côte d'ivoire": 'ivory coast',
    'gambia': 'the gambia',
    'holland': 'the netherlands',
    'ksa': 'saudi arabia',
    'macedonia': 'north macedonia',
    'micronesia (federated states of)': 'micronesia',
    'myanmar': 'myanmar (burma)',
    'myanmar (burma)': 'myanmar (burma)',
    'palestine, state of': 'occupied palestinian territories',
    'saint helena, ascension and tristan da cunha': 'saint helena',
    'saint kitts and nevis': 'st kitts and nevis',
    'saint lucia': 'st lucia',
    'saint vincent and the grenadines': 'st vincent',
    'st martin': 'saint-martin (french part)',
    'swaziland': 'eswatini',
    'uae': 'united arab emirates',
    'usa': 'united states',
    'united states of america': 'united states',
    'united states minor outlying islands': 'united states',
    'virgin islands (british)': 'british virgin islands',
    'virgin islands (u.s.)': 'united states virgin islands',
}
split_mappings = {
    'Netherlands Antilles': [
        'Bonaire',
        'Saint Eustatius',
        'Saba',
        'Curaçao',
        'Sint Maarten (Dutch part)',
    ],
}


def _standardise_country(country, choices, lower_choices):
    lower_value = country.lower()
    # ignore regions
    if lower_value in regions:
        lower_value = '0' * len(lower_value)
    # reformat countries
    for key, value in replacements.items():
        if key == lower_value:
            lower_value = value
            break

    # check for direct match
    index = lower_choices.index(lower_value) if lower_value in lower_choices else None
    if index:
        top_matches = [(choices[index], 100)]

    # check for fuzzy match, include first result and other
    # matches with 90 or higher similarity
    else:
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
        top_matches = [
            (
                match[0],
                max(
                    match[1]
                    - math.ceil(abs(len(country) - len(match[0])) / (max(len(country), 1))) * 4,
                    0,
                )
                if match[0].lower().replace('the ', '') not in country.lower()
                else match[1],
            )
            for match in top_matches
        ]

    # split if group of countries
    split_top_matches = []
    for country, score in top_matches:
        if country in split_mappings.keys():
            split_top_matches = split_top_matches + [
                (split_country, score) for split_country in split_mappings[country]
            ]
        else:
            split_top_matches.append((country, score))

    return split_top_matches


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
    flask_app.dbi.execute_statement(stmt)
