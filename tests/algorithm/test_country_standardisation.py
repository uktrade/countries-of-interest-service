import app.algorithm.country_standardisation as mapper
from app.algorithm.country_standardisation.steps import _standardize_country

from tests.utils import rows_equal_query_results


def test_country_mapping(
    add_datahub_export_to_countries,
    add_datahub_future_interest_countries,
    add_datahub_omis,
    add_export_wins,
    add_country_territory_registry,
):

    add_datahub_export_to_countries(
        [
            {
                'company_id': '25262ffe-e062-49af-a620-a84d4f3feb8b',
                'country_iso_alpha2_code': 'AF',
                'country': 'afganistan',
                'id': 0,
            }
        ]
    )

    add_datahub_future_interest_countries(
        [
            {
                'company_id': 'd584c5e2-ef16-4aba-91d4-71949078831f',
                'country_iso_alpha2_code': 'AD',
                'country': 'Andorra',
                'id': 0,
            }
        ]
    )
    add_datahub_omis(
        [
            {
                'company_id': '7ff93060-1f05-4bbc-a58a-65ae7d7e8ba8',
                'created_date': '2009-10-10',
                'id': 'a0e38b4f-f4c9-4ebc-b196-208972268efb',
                'market': 'usa',
                'sector': 'Aerospace',
            }
        ]
    )
    add_export_wins(
        [
            {
                'company_id': '04fba8d6-5e83-425c-a22e-69879767a26c',
                'country': 'uae',
                'id': 'ffa75985-7bc0-4e9f-8d58-28a7f234b7fc',
                'timestamp': '2009-10-10 12:12:12',
            },
            {
                'company_id': '5b1f6cdb-1347-4452-9fac-41163cb52841',
                'country': 'unknown',
                'id': '2f3e1939-fba7-4e4e-b53b-3fd829c7d606',
                'timestamp': '2009-10-10 12:12:12',
            },
        ]
    )

    # Populate registry
    countries = [
        ('AF', 'Afghanistan'),
        ('AL', 'Albania'),
        ('DZ', 'Algeria'),
        ('AD', 'Andorra'),
        ('AO', 'Angola'),
        ('AG', 'Antigua and Barbuda'),
        ('BE', 'Belgium'),
        ('CL', 'Chile'),
        ('FR', 'France'),
        ('NZ', 'New Zealand'),
        ('CA', 'Canada'),
        ('US', 'United States'),
        ('CN', 'China'),
        ('DE', 'Germany'),
        ('BS', 'The Bahamas'),
        ('IN', 'India'),
        ('JP', 'Japan'),
        ('US', 'United States'),
        ('AU', 'Australia'),
        ('NZ', 'New Zealand'),
        ('CA', 'Canada'),
        ('AE', 'United Arab Emirates'),
    ]
    country_territory_entries = []
    for iso_alpha2_code, country in countries:
        entry = {'id': iso_alpha2_code, 'name': country}
        country_territory_entries.append(entry)
    add_country_territory_registry(country_territory_entries)

    # Standardise
    mapper.standardise_countries()

    # TODO: uncomment once export wins is included
    expected_rows = [
        (1, 'afganistan', 'Afghanistan', 91),
        (2, 'Andorra', 'Andorra', 100),
        # (3, 'uae', 'United Arab Emirates', 100),
        # (3, 'unknown', 'Angola', 27),
        (3, 'usa', 'United States', 100),
    ]

    assert rows_equal_query_results(
        expected_rows, f'SELECT * FROM "{mapper.output_schema}"."{mapper.output_table}"'
    )


def test_standardise_country():

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
    assert _standardise_country('Belgium', choices, lower_choices) == [('Belgium', 100)]
    assert _standardise_country('uae', choices, lower_choices) == [
        ('United Arab Emirates', 100)
    ]
    assert _standardise_country('usa', choices, lower_choices) == [
        ('United States', 100)
    ]
    assert _standardise_country('Africa Belgium', choices, lower_choices) == [
        ('Belgium', 90)
    ]
    assert _standardise_country('Africa Belgium Austria', choices, lower_choices) == [
        ('Belgium', 90),
        ('Austria', 90),
    ]
    assert _standardise_country('Africa and Belgium', choices, lower_choices) == [
        ('Belgium', 90)
    ]
    assert _standardise_country('german', choices, lower_choices) == [('Germany', 88)]
    assert _standardise_country('Belgiun', choices, lower_choices) == [('Belgium', 86)]
    assert _standardise_country('Belgiu', choices, lower_choices) == [('Belgium', 88)]
    assert _standardise_country(
        'democratic republic of congo', choices, lower_choices
    ) == [('Congo (Democratic Republic)', 91)]
    assert _standardise_country('bahamas', choices, lower_choices) == [
        ('The Bahamas', 90)
    ]
    assert _standardise_country('ao', choices, lower_choices) == [('Laos', 86)]

    # test mismatches
    assert _standardise_country('Africa', choices, lower_choices) == [('Belgium', 0)]
    assert _standardise_country('Unknown', choices, lower_choices) == [
        ('United Arab Emirates', 19)
    ]
    assert _standardise_country('africa (any)', choices, lower_choices) == [
        ('Germany', 35)
    ]
    assert _standardise_country('a', choices, lower_choices) == [
        ('Austria', 66),
        ('Germany', 66),
        ('Laos', 78),
    ]
    assert _standardise_country('anywhere in the world', choices, lower_choices) == [
        ('The Bahamas', 82)
    ]
