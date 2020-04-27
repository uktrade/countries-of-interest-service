import datetime

import pandas as pd
from data_engineering.common.api.utils import response_orientation_decorator, to_web_dict
from data_engineering.common.views import ac, json_error
from flask import current_app as flask_app
from flask import render_template, request
from werkzeug.exceptions import BadRequest

from app.api.tasks import populate_database_task
from app.db.models import internal as internal_models
from app.db.models.internal import CountriesAndSectorsInterest
from app.sso.token import login_required


@json_error
@response_orientation_decorator
@ac.authentication_required
@ac.authorization_required
def get_company_countries_and_sectors_of_interest(orientation):
    pagination_size = flask_app.config['app']['pagination_size']
    next_id = request.args.get('next-id')
    service_company_ids = request.args.getlist('service-company-id')
    company_match_ids = request.args.getlist('company-match-id')
    countries = request.args.getlist('country')
    sectors = request.args.getlist('sector')
    types = request.args.getlist('type')
    services = request.args.getlist('service')
    sources = request.args.getlist('source')

    where = ''
    values = (
        countries + sectors + service_company_ids + company_match_ids + types + sources + services
    )
    if len(countries) == 1:
        where = 'where country=%s'
    elif len(countries) > 1:
        where = 'where country in (' + ','.join('%s' for i in range(len(countries))) + ')'
    if len(sectors) == 1:
        where = where + ' and' if where != '' else 'where'
        where = where + ' sector=%s'
    elif len(sectors) > 1:
        where = where + ' and' if where != '' else 'where'
        where = where + ' sector in (' + ','.join(['%s' for i in range(len(sectors))]) + ')'
    if len(service_company_ids) == 1:
        where = where + ' and' if where != '' else 'where'
        where = where + ' service_company_id=%s'
    elif len(service_company_ids) > 1:
        where = where + ' and' if where != '' else 'where'
        where = (
            where
            + ' service_company_id in ('
            + ','.join(['%s' for i in range(len(service_company_ids))])
            + ')'
        )
    if len(company_match_ids) == 1:
        where = where + ' and' if where != '' else 'where'
        where = where + ' company_match_id=%s'
    elif len(company_match_ids) > 1:
        where = where + ' and' if where != '' else 'where'
        where = (
            where
            + ' company_match_id in ('
            + ','.join(['%s' for i in range(len(company_match_ids))])
            + ')'
        )
    if len(types) == 1:
        where = where + ' and' if where != '' else 'where'
        where = where + ' type=%s'
    elif len(types) > 1:
        where = where + ' and' if where != '' else 'where'
        where = where + ' type in (' + ','.join(['%s' for i in range(len(types))]) + ')'
    if len(sources) == 1:
        where = where + ' and' if where != '' else 'where'
        where = where + ' source=%s'
    elif len(sources) > 1:
        where = where + ' and' if where != '' else 'where'
        where = where + ' source in (' + ','.join(['%s' for i in range(len(sources))]) + ')'
    if len(services) == 1:
        where = where + ' and' if where != '' else 'where'
        where = where + ' service=%s'
    elif len(services) > 1:
        where = where + ' and' if where != '' else 'where'
        where = where + ' service in (' + ','.join(['%s' for i in range(len(services))]) + ')'
    if next_id is not None:
        where = where + ' and' if where != '' else 'where'
        where = where + ' id >= %s'
        values = values + [next_id]

    sql_query = f'''
        select
            id,
            service_company_id,
            company_match_id,
            country,
            sector,
            type,
            service,
            source,
            source_id,
            timestamp
        from {CountriesAndSectorsInterest.get_fq_table_name()}
        {where}
        order by id
        limit {pagination_size} + 1
    '''

    df = flask_app.dbi.execute_query(sql_query, data=values, df=True)
    if len(df) == pagination_size + 1:
        next_ = '{}{}?'.format(request.host_url[:-1], request.path)
        next_ += '&'.join(
            ['service-company-id={}'.format(company_id) for company_id in service_company_ids]
        )
        next_ += '&'.join(
            [
                'company-match-id={}'.format(company_match_id)
                for company_match_id in company_match_ids
            ]
        )
        next_ += '&'.join(['country={}'.format(country) for country in countries])
        next_ += '&'.join(['sector={}'.format(sector) for sector in sectors])
        next_ += '&'.join(['type={}'.format(type) for type in types])
        next_ += '&'.join(['service={}'.format(service) for service in services])
        next_ += '&'.join(['source={}'.format(source) for source in sources])
        next_ += '&' if next_[-1] != '?' else ''
        next_ += 'orientation={}'.format(orientation)
        next_ += '&next-id={}'.format(df['id'].values[-1])
        df = df[:-1]
    else:
        next_ = None
    web_dict = to_web_dict(df.iloc[:, 1:], orientation)
    web_dict['next'] = next_
    return flask_app.make_response(web_dict)


@login_required
def get_index():
    last_updated = None
    if flask_app.dbi.table_exists('public', 'etl_runs'):
        sql = 'select max(timestamp) from etl_runs'
        df = flask_app.dbi.execute_query(sql, df=True)
        last_updated = pd.to_datetime(df.values[0][0])
    if last_updated is None:
        last_updated = 'Database not yet initialised'
    else:
        last_updated = last_updated.strftime('%Y-%m-%d %H:%M:%S')
    return render_template('index.html', last_updated=last_updated)


@json_error
@ac.authentication_required
@ac.authorization_required
def populate_database():
    drop_table = 'drop-table' in request.args
    force_update = 'force-update' in request.args
    extractors = []
    if 'extractors' in request.args:
        extractors = request.args['extractors'].split(',')

    tasks = []
    if 'tasks' in request.args:
        tasks = request.args['tasks'].split(',')

    sql = 'select * from etl_status'

    df = flask_app.dbi.execute_query(sql, df=True)
    if force_update is True or len(df) == 0 or df['status'].values[0] == 'SUCCESS':
        populate_database_task.delay(drop_table, extractors, tasks)
        sql = 'delete from etl_status'
        flask_app.dbi.execute_statement(sql)
        sql = '''insert into etl_status (status, timestamp) values (%s, %s)'''
        flask_app.dbi.execute_statement(sql, data=['RUNNING', datetime.datetime.now()])
        return flask_app.make_response({'status': 200, 'message': 'started populate_database task'})
    else:
        timestamp = df['timestamp'].values[0]
        response = {
            'status': 200,
            'message': f"populate_database task already running since: {timestamp}",
        }
        return flask_app.make_response(response)


@login_required
def get_data_visualisation_data(field):
    date_trunc = 'quarter'
    data_source = request.args['data-source']
    interests_table = internal_models.CountriesAndSectorsInterest.__tablename__
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

    df = flask_app.dbi.execute_query(sql, df=True)
    df_top = df.groupby(field)[['n_interests_cumulative']].max()
    df_top = df_top.reset_index()
    df_top = df_top.sort_values('n_interests_cumulative', ascending=False)

    output = {
        'nInterests': to_web_dict(df)['results'],
        'top': df_top[field].tolist(),
    }

    return output


@login_required
def data_visualisation():
    return render_template('data-visualisation.html')
