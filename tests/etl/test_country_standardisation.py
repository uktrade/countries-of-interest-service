from app.db.models.internal import StandardisedCountries
from app.etl.tasks.country_standardisation import PopulateStandardisedCountriesTask


def test(
    add_datahub_company_export_country,
    add_datahub_company_export_country_history,
    add_datahub_omis,
    add_export_wins,
    add_country_territory_registry,
):

    add_datahub_company_export_country(
        [
            {
                'company_export_country_id': '9c83fe1f-24c9-4f44-8d04-3a6b82d6ca34',
                'company_id': '25262ffe-e062-49af-a620-a84d4f3feb8b',
                'country': 'United Kingdom',
                'country_iso_alpha2_code': 'UK',
                'created_on': '2020-01-01',
                'modified_on': '2020-01-02',
                'status': 'currently_exporting'
            },
        ]
    )

    add_datahub_company_export_country_history(
        [
            {
                'company_id': 'd584c5e2-ef16-4aba-91d4-71949078831f',
                'country': 'germany',
                'country_iso_alpha2_code': 'DE',
                'history_date': '2020-01-01',
                'history_id': 'd413d348-8828-48a7-88a1-1e8a1d7a2f1f',
                'history_type': 'insert',
                'status': 'future_interest',
            }
        ]
    )

    add_datahub_omis(
        [
            {
                'company_id': 'f1fe0161-a3ed-4566-b920-1935c108ae2d',
                'created_date': '2019-01-01 01:00:00',
                'datahub_omis_order_id': '56c47b42-0306-4e74-a915-e279f81be0b5',
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
        ('DE', 'Germany'),
    ]
    country_territory_entries = []
    for iso_alpha2_code, country in countries:
        entry = {'country_iso_alpha2_code': iso_alpha2_code, 'name': country}
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
    assert standardised_countries[1].country == 'germany'
    assert standardised_countries[1].standardised_country == 'Germany'

    assert standardised_countries[2].id == 3
    assert standardised_countries[2].country == 'uae'
    assert standardised_countries[2].standardised_country == 'United Arab Emirates'

    assert standardised_countries[3].id == 4
    assert standardised_countries[3].country == 'United Kingdom'
    assert standardised_countries[3].standardised_country == 'United Kingdom'
