from app.algorithm.country_standardisation import steps as sql
from app.db import db_utils
from app.db.models.internal import StandardisedCountries
from app.utils import log

output_schema = StandardisedCountries.get_schema()
output_table = StandardisedCountries.__tablename__


def standardise_countries():
    mapper = CountryMapper()
    mapper.map()


class CountryMapper:
    @log.write('rebuilding country mapping')
    def map(self):
        db_utils.execute_statement(
            "SET statement_timeout TO '1h' "
        )  # If a query takes longer over 1hour, stop it!
        countries = sql.extract_interested_exported_countries()
        sql.create_standardised_interested_exported_country_table(
            countries, output_schema, output_table
        )
