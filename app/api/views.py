import datetime
from functools import wraps

import pandas as pd
import redis
from flask import current_app as flask_app
from flask import jsonify, make_response, render_template, request
from flask.blueprints import Blueprint
from werkzeug.exceptions import BadRequest, NotFound, Unauthorized

from app.api.access_control import AccessControl
from app.api.tasks import populate_database_task
from app.api.utils import response_orientation_decorator, to_web_dict
from app.db.db_utils import execute_query, execute_statement, table_exists
from app.db.models import internal as internal_models
from app.db.models.internal import CountriesAndSectorsInterest
from app.db.models.internal import HawkUsers
from app.sso.token import login_required

api = Blueprint(name="api", import_name=__name__)
ac = AccessControl()


@api.route('/api/v1/get-data-visualisation-data/<field>')
def get_data_visualisation_data(field):
    date_trunc = request.args.get('date_trunc', 'quarter')
    data_source = request.args['data-source']
    interests_table = 'public.countries_of_interest_dataset'
    from app.config import constants

    valid_data_sources = [
        constants.Source.EXPORT_WINS.value,
        constants.Source.DATAHUB_INTERACTIONS.value,
        constants.Source.DATAHUB_OMIS.value,
    ]
    valid_fields = ['country', 'sector']

    if field not in valid_fields:
        raise BadRequest(f'invalid field: {field}, valid values: {valid_fields}')

    if data_source not in valid_data_sources:
        raise BadRequest(
            f'invalid data-source: {data_source}, ' f'valid values: {valid_data_sources}'
        )

    if field == 'sector' and data_source == constants.Source.DATAHUB_INTERACTIONS.value:
        raise BadRequest(
            f'invalid args: data-source: {constants.Source.DATAHUB_INTERACTIONS.value}'
            ' not supported by sector'
        )

    sql = '''
    with n_interests as (
        select
            date_trunc('{date_trunc}', timestamp) as date,
            {field},
            count(1)

        from {interests_table}

        where source = '{data_source}'
            and {field} != ''
            and {field} is not null
            and {field} != 'United Kingdom'

        group by 1, 2

    ), dates as (
        select distinct date from n_interests
    ), fields as (
        select distinct {field} from n_interests
    ), zero_inflated as (
        select
            date,
            {field},
            coalesce(count, 0) as n_interests

        from dates
            left join fields on 1=1
            left join n_interests using (date, {field})

    ), cumulative as (
        select
            date,
            {field},
            sum(n_interests::int) over (partition by {field} order by date)
                as n_interests_cumulative

        from zero_inflated

    ), total_interest as (
        select
            date,
            sum(n_interests) as total_interest

        from zero_inflated

       group by 1

    ), total_interest_cumulative as (
        select
            date,
            sum(total_interest::int) over (order by date) as total_interest_cumulative

        from total_interest

    ), results as (
        select
            date,
            {field},
            n_interests,
            n_interests::float / total_interest as share_of_interest,
            n_interests_cumulative,
            n_interests_cumulative::float / total_interest_cumulative
                as share_of_interest_cumulative

        from zero_inflated
            join total_interest using (date)
            join cumulative using (date, {field})
            join total_interest_cumulative using (date)

    )

    select * from results order by (date, {field})

    '''.format(
        date_trunc=date_trunc,
        data_source=data_source,
        field=field,
        interests_table=interests_table,
    )

    df = execute_query(sql)
    df_top = df.groupby(field)[['n_interests_cumulative']].max()
    df_top = df_top.reset_index()
    df_top = df_top.sort_values('n_interests_cumulative', ascending=False)

    output = {
        'nInterests': to_web_dict(df)['results'],
        'top': df_top[field].tolist(),
    }

    return output


@api.route('/')
def data_visualisation():
    return render_template('data-visualisation.html')


@api.route('/healthcheck/', methods=["GET"])
def healthcheck():
    return jsonify({"status": "OK"})
