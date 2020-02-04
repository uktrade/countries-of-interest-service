import csv
import time

from flask import current_app as flask_app

from app.db.models import external as external_models
from app.db.models import internal as internal_models


def get_coi_csv(output, limit=None):
    coi_and_sectors_fields = [
        'service_company_id',
        'company_match_id',
        'country',
        'sector',
        'type',
        'service',
        'source',
        'source_id',
        'timestamp',
    ]

    data_sets = [
        (internal_models.CountriesAndSectorsInterest, coi_and_sectors_fields, True),
    ]

    return output_csv(data_sets, output, 'timestamp', limit)


def output_csv(data_sets, output, order_by, limit):
    for db_model, db_fields, set_headers in data_sets:
        populate_csv(
            output, db_model, db_fields, limit, order_by=order_by, set_headers=set_headers,
        )
    return output


def populate_csv(stream, db_model, db_fields, limit, order_by='timestamp', set_headers=False):
    writer = csv.writer(stream)

    session = flask_app.db.session

    if set_headers:
        writer.writerow(db_fields)

    if getattr(db_model, '__tablename__', None) == 'coi_countries_of_interest':
        query = session.query(db_model).filter(db_model.source != 'omis').order_by(order_by)
    else:
        query = session.query(db_model).order_by(order_by)

    paginator = Paginator(query, 10000, limit=limit)
    flask_app.logger.info(f'Found {paginator.total_count} total items')
    flask_app.logger.info(f'Processing only first {paginator.required_items_count} items')
    flask_app.logger.info(f'Total number of pages to process {paginator.required_pages}')

    start_time = time.time()
    for result in paginator.get_all_pages():
        for item in result.items:
            row = [_get_field_value(item, field) for field in db_fields]
            writer.writerow(row)
    flask_app.logger.info(
        'Finished: Took {0:0.1f} seconds to process'.format(time.time() - start_time)
    )


def _get_field_value(item, field):
    session = flask_app.db.session

    if field == 'interaction_notes':
        return ""
        model = external_models.Interactions
        item = session.query(model).filter_by(datahub_interaction_id=item.interaction_id).first()
        return getattr(item, 'notes', "")

    fields_with_defaults = {
        'interaction_source': 'datahub interaction',
    }
    default = fields_with_defaults.get(field, "")
    return getattr(item, field, default)


class Paginator:
    def __init__(self, query, per_page, limit=None):
        self.query = query
        self.per_page = per_page
        self.limit = limit

    @property
    def required_items_count(self):
        total_count = self.total_count
        if self.limit:
            return min(total_count, self.limit)
        return total_count

    @property
    def total_count(self):
        return self.query.count()

    @property
    def total_pages(self):
        count = int(self.total_count)
        return self.calculate_pages(count)

    def calculate_pages(self, count):
        pages = int(count / self.per_page)
        if count % self.per_page:
            pages += 1
        return pages

    @property
    def required_pages(self):
        count = int(self.total_count)
        if self.limit and count > self.limit:
            count = self.limit
        return self.calculate_pages(count)

    def get_page(self, page_number):
        return self.query.paginate(page=page_number, per_page=self.per_page, error_out=False)

    def get_all_pages(self):
        for page_number in range(1, self.required_pages + 1):
            flask_app.logger.info(f'Processing page: {page_number}')
            yield self.get_page(page_number)
