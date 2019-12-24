import app.algorithm.country_standardisation as mapper
from app.algorithm.country_standardisation.sql_statements import _standardize_country

from tests.utils import rows_equal_query_results


def test_country_mapping(
    add_countries_and_sectors_of_interest,
    add_company_countries_of_interest,
    add_company_export_countries,
    add_country_territory_registry,
):

    # Test mapping
    add_countries_and_sectors_of_interest(
        [
            {
                'company_id': '1',
                'country_of_interest': 'afganistan',
                'sector_of_interest': 'sector1',
                'source': 'source1',
                'source_id': 'source_id1',
                'timestamp': '2009-10-10 12:12:12',
            }
        ]
    )
    add_company_countries_of_interest(
        [
            {
                'company_id': '1',
                'country_of_interest': 'Andorra',
                'source': 'source1',
                'source_id': 'source_id1',
                'timestamp': '2009-10-10 12:12:12',
            },
            {
                'company_id': '2',
                'country_of_interest': 'usa',
                'source': 'source1',
                'source_id': 'source_id2',
                'timestamp': '2009-10-10 12:12:12',
            },
        ]
    )
    add_company_export_countries(
        [
            {
                'company_id': '1',
                'export_country': 'uae',
                'source': 'source1',
                'source_id': 'source_id1',
                'timestamp': '2009-10-10 12:12:12',
            },
            {
                'company_id': '2',
                'export_country': 'unknown',
                'source': 'source1',
                'source_id': 'source_id2',
                'timestamp': '2009-10-10 12:12:12',
            },
        ]
    )

    # Populate registry
    countries = [
        'Afghanistan',
        'Albania',
        'Algeria',
        'Andorra',
        'Angola',
        'Antigua and Barbuda',
        'Belgium',
        'Chile',
        'France',
        'New Zealand',
        'Canada',
        'United States',
        'China',
        'Germany',
        'The Bahamas',
        'India',
        'Japan',
        'United States',
        'Australia',
        'New Zealand',
        'Canada',
        'United Arab Emirates',
    ]
    country_territory_entries = []
    for i, country in enumerate(countries):
        entry = {'id': i, 'name': country}
        country_territory_entries.append(entry)
    add_country_territory_registry(country_territory_entries)

    # Standardize
    mapper.standardize_countries()

    expected_rows = [
        (1, 'afganistan', 'Afghanistan', 91),
        (2, 'Andorra', 'Andorra', 100),
        (3, 'uae', 'United Arab Emirates', 100),
        (4, 'unknown', 'Angola', 27),
        (5, 'usa', 'United States', 100),
    ]

    assert rows_equal_query_results(
        expected_rows, f'SELECT * FROM "{mapper.output_schema}"."{mapper.output_table}"'
    )


def test_standardize_country():

    choices = [
        'Belgium',
        'Austria',
        'South Africa',
        'Germany',
        'The Bahamas',
        'United Arab Emirates',
        'United States',
        'Congo (Democratic Republic)',
        'Laos',
    ]
    lower_choices = [choice.lower() for choice in choices]

    # test sensible matches (threshold > 85)
    assert _standardize_country('Belgium', choices, lower_choices) == [('Belgium', 100)]
    assert _standardize_country('uae', choices, lower_choices) == [
        ('United Arab Emirates', 100)
    ]
    assert _standardize_country('usa', choices, lower_choices) == [
        ('United States', 100)
    ]
    assert _standardize_country('Africa Belgium', choices, lower_choices) == [
        ('Belgium', 90)
    ]
    assert _standardize_country('Africa Belgium Austria', choices, lower_choices) == [
        ('Belgium', 90),
        ('Austria', 90),
    ]
    assert _standardize_country('Africa and Belgium', choices, lower_choices) == [
        ('Belgium', 90)
    ]
    assert _standardize_country('german', choices, lower_choices) == [('Germany', 88)]
    assert _standardize_country('Belgiun', choices, lower_choices) == [('Belgium', 86)]
    assert _standardize_country('Belgiu', choices, lower_choices) == [('Belgium', 88)]
    assert _standardize_country(
        'democratic republic of congo', choices, lower_choices
    ) == [('Congo (Democratic Republic)', 91)]
    assert _standardize_country('bahamas', choices, lower_choices) == [
        ('The Bahamas', 90)
    ]
    assert _standardize_country('ao', choices, lower_choices) == [('Laos', 86)]

    # test mismatches
    assert _standardize_country('Africa', choices, lower_choices) == [('Belgium', 0)]
    assert _standardize_country('Unknown', choices, lower_choices) == [
        ('United Arab Emirates', 19)
    ]
    assert _standardize_country('africa (any)', choices, lower_choices) == [
        ('Germany', 35)
    ]
    assert _standardize_country('a', choices, lower_choices) == [
        ('Austria', 66),
        ('Germany', 66),
        ('Laos', 78),
    ]
    assert _standardize_country('anywhere in the world', choices, lower_choices) == [
        ('The Bahamas', 82)
    ]
