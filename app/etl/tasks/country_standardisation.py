import app.db.models as db
import app.algorithm.country_standardisation.sql_statements as country_standardisation


class PopulateStandardisedCountriesTask:
    def __call__(drop_table=True):
        if drop_table is True:
            # todo
            pass
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
