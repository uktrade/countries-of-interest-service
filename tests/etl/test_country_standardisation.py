from app.db.models import StandardisedCountries
from app.etl.tasks.country_standardisation import PopulateStandardisedCountriesTask


def test(
    add_datahub_export_to_countries,
    add_datahub_future_interest_countries,
    add_datahub_omis,
    add_export_wins,
    add_country_territory_registry,
):
    add_datahub_export_to_countries(
        [
            {
                'company_id': 'f17645ba-c38e-4215-b09e-47451edb9125',
                'country_iso_alpha2_code': 'UK',
                'id': 1,
            }
        ]
    )

    add_datahub_future_interest_countries(
        [
            {
                'company_id': '2f6a340e-07b1-4bc0-9852-9138fc93249e',
                'country_iso_alpha2_code': 'DE',
                'id': 1,
            }
        ]
    )

    add_datahub_omis(
        [
            {
                'company_id': 'f1fe0161-a3ed-4566-b920-1935c108ae2d',
                'created_date': '2019-01-01 01:00:00',
                'id': '56c47b42-0306-4e74-a915-e279f81be0b5',
                'market': 'AG',
                'sector': 'Aerospace',
            }
        ]
    )

    add_export_wins(
        [
            {
                'company_id': 'ee4a937e-0818-4b8a-8bbf-530cd11293db',
                'country': 'uae',
                'id': 'ee4a937e-0818-4b8a-8bbf-530cd11293db',
                'timestamp': '2019-01-02 07:00:00',
            }
        ]
    )

    # Populate registry
    countries = [
        ('UE', 'United Arab Emirates'),
        ('UK', 'United Kingdom'),
    ]
    country_territory_entries = []
    for iso_alpha2_code, country in countries:
        entry = {'id': iso_alpha2_code, 'name': country}
        country_territory_entries.append(entry)
    add_country_territory_registry(country_territory_entries)

    task = PopulateStandardisedCountriesTask()
    task()

    standardised_countries = StandardisedCountries.query.all()

    assert len(standardised_countries) == 4

    assert standardised_countries[0].id == 1
    assert standardised_countries[0].country == 'AG'
    assert standardised_countries[0].standardised_country == 'United Kingdom'

    assert standardised_countries[1].id == 2
    assert standardised_countries[1].country == 'DE'
    assert standardised_countries[1].standardised_country == 'United Kingdom'

    assert standardised_countries[2].id == 3
    assert standardised_countries[2].country == 'uae'
    assert standardised_countries[2].standardised_country == 'United Arab Emirates'

    assert standardised_countries[3].id == 4
    assert standardised_countries[3].country == 'UK'
    assert standardised_countries[3].standardised_country == 'United Kingdom'
