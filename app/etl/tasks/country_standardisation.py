import app.algorithm.country_standardisation.steps as country_standardisation
from app.config import constants
from app.db.db_utils import drop_table
from app.db.models.internal import StandardisedCountries


class PopulateStandardisedCountriesTask:

    name = constants.Task.STANDARDISE_COUNTRIES.value

    def __init__(self, **kwargs):
        super().__init__()

    def __call__(self):
        drop_table(StandardisedCountries.__tablename__)
        countries = country_standardisation.extract_interested_exported_countries()
        country_standardisation.create_standardised_interested_exported_country_table(
            countries,
            StandardisedCountries.__table_args__['schema'],
            StandardisedCountries.__tablename__,
        )
        n_rows = len(StandardisedCountries.query.all())
        return {
            'status': 200,
            'rows': n_rows,
            'task': self.name,
        }
