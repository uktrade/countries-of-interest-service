import app.algorithm.country_standardisation.steps as country_standardisation
import app.db.models as db
from app.db.db_utils import drop_table


class PopulateStandardisedCountriesTask:

    name = 'PopulateStandardisedCountriesTask'

    def __call__(self):
        drop_table(db.StandardisedCountries.__tablename__)
        countries = country_standardisation.extract_interested_exported_countries()
        country_standardisation.create_standardised_interested_exported_country_table(
            countries,
            db.StandardisedCountries.__table_args__['schema'],
            db.StandardisedCountries.__tablename__,
        )
        n_rows = len(db.StandardisedCountries.query.all())
        return {
            'status': 'success',
            'rows': n_rows,
            'table': db.StandardisedCountries.__tablename__,
        }
