import datetime
import logging
from functools import wraps

from flask import current_app as flask_app, make_response
from flask import jsonify, render_template, request
from flask.blueprints import Blueprint

import pandas as pd

import redis

from werkzeug.exceptions import BadRequest, NotFound, Unauthorized

from app.api.access_control import AccessControl
from app.api.tasks import populate_database_task
from app.api.utils import response_orientation_decorator, to_web_dict
from app.db.db_utils import execute_query, execute_statement, table_exists
from app.db.models.internal import HawkUsers
from app.sso.token import login_required

api = Blueprint(name="api", import_name=__name__)
ac = AccessControl()


@ac.client_key_loader
def get_client_key(client_id):
    client_key = HawkUsers.get_client_key(client_id)
    if client_key:
        return client_key
    else:
        raise LookupError()


@ac.client_scope_loader
def get_client_scope(client_id):
    client_scope = HawkUsers.get_client_scope(client_id)
    if client_scope:
        return client_scope
    else:
        raise LookupError()


@ac.nonce_checker
def seen_nonce(sender_id, nonce, timestamp):
    key = f'{sender_id}:{nonce}:{timestamp}'
    try:
        if flask_app.cache.get(key):
            # We have already processed this nonce + timestamp.
            return True
        else:
            # Save this nonce + timestamp for later.
            flask_app.cache.set(key, 'True', ex=300)
            return False
    except redis.exceptions.ConnectionError as e:
        logging.error(f'failed to connect to caching server: {str(e)}')
        return True


def json_error(f):
    @wraps(f)
    def error_handler(*args, **kwargs):
        try:
            response = f(*args, **kwargs)
        except NotFound:
            response = jsonify({})
            response.status_code = 404
        except BadRequest as e:
            response = jsonify({'error': e.description})
            response.status_code = 400
        except Unauthorized:
            response = make_response('')
            response.status_code = 401
        except Exception as e:
            logging.error(f'unexpected exception for API request: {str(e)}')
            response = make_response('')
            response.status_code = 500
        return response

    return error_handler


# views
@api.route('/api/v1/get-company-countries-and-sectors-of-interest')
@json_error
@response_orientation_decorator
@ac.authentication_required
@ac.authorization_required
def get_company_countries_and_sectors_of_interest(orientation):
    pagination_size = flask_app.config['app']['pagination_size']
    next_source = request.args.get('next-source')
    next_source_id = request.args.get('next-source-id')
    company_ids = request.args.getlist('company-id')
    countries = request.args.getlist('country')
    sectors = request.args.getlist('sector')
    sources = request.args.getlist('source')

    where = ''
    values = countries + sectors + sources + company_ids
    if len(countries) == 1:
        where = 'where country_of_interest=%s'
    elif len(countries) > 1:
        where = (
            'where country_of_interest in ('
            + ','.join('%s' for i in range(len(countries)))
            + ')'
        )
    if len(sectors) == 1:
        where = where + ' and' if where != '' else 'where'
        where = where + ' sector_of_interest=%s'
    elif len(sectors) > 1:
        where = where + ' and' if where != '' else 'where'
        where = (
            where
            + ' sector_of_interest in ('
            + ','.join(['%s' for i in range(len(sectors))])
            + ')'
        )
    if len(company_ids) == 1:
        where = where + ' and' if where != '' else 'where'
        where = where + ' company_id=%s'
    elif len(company_ids) > 1:
        where = where + ' and' if where != '' else 'where'
        where = (
            where
            + ' company_id in ('
            + ','.join(['%s' for i in range(len(company_ids))])
            + ')'
        )
    if len(sources) == 1:
        where = where + ' and' if where != '' else 'where'
        where = where + ' source=%s'
    elif len(sources) > 1:
        where = where + ' and' if where != '' else 'where'
        where = (
            where + ' source in (' + ','.join(['%s' for i in range(len(sources))]) + ')'
        )
    if next_source is not None and next_source_id is not None:
        where = where + ' and' if where != '' else 'where'
        where = where + ' (source, source_id) >= (%s, %s)'
        values = values + [next_source, next_source_id]

    sql_query = f'''
        select
          company_id,
          country_of_interest,
          standardised_country,
          sector_of_interest,
          source,
          source_id,
          timestamp
        from coi_countries_and_sectors_of_interest
        {where}
        order by (source, source_id)
        limit {pagination_size} + 1
    '''
    df = execute_query(sql_query, data=values)
    if len(df) == pagination_size + 1:
        next_ = '{}{}?'.format(request.host_url[:-1], request.path)
        next_ += '&'.join(['country={}'.format(country) for country in countries])
        next_ += '&'.join(['source={}'.format(source) for source in sources])
        next_ += '&' if next_[-1] != '?' else ''
        next_ += 'orientation={}'.format(orientation)
        next_ += '&next-source={}&next-source-id={}'.format(
            df['source'].values[-1], df['source_id'].values[-1],
        )
        df = df[:-1]
    else:
        next_ = None
    web_dict = to_web_dict(df, orientation)
    web_dict['next'] = next_
    return flask_app.make_response(web_dict)


@api.route('/api/v1/get-company-countries-of-interest')
@json_error
@response_orientation_decorator
@ac.authentication_required
@ac.authorization_required
def get_company_countries_of_interest(orientation):
    pagination_size = flask_app.config['app']['pagination_size']
    next_source = request.args.get('next-source')
    next_source_id = request.args.get('next-source-id')
    company_ids = request.args.getlist('company-id')
    countries = request.args.getlist('country')
    sources = request.args.getlist('source')

    values = countries + sources + company_ids
    where = ''
    if len(countries) == 1:
        where = 'where country_of_interest=%s'
    elif len(countries) > 1:
        where = (
            'where country_of_interest in ('
            + ','.join('%s' for i in range(len(countries)))
            + ')'
        )
    if len(company_ids) == 1:
        where = where + ' and' if where != '' else 'where'
        where = where + ' company_id=%s'
    elif len(company_ids) > 1:
        where = where + ' and' if where != '' else 'where'
        where = (
            where
            + ' company_id in ('
            + ','.join(['%s' for i in range(len(company_ids))])
            + ')'
        )
    if len(sources) == 1:
        where = where + ' and' if where != '' else 'where'
        where = where + ' source=%s'
    elif len(sources) > 1:
        where = where + ' and' if where != '' else 'where'
        where = (
            where + ' source in (' + ','.join(['%s' for i in range(len(sources))]) + ')'
        )
    if next_source is not None and next_source_id is not None:
        where = where + ' and' if where != '' else 'where'
        where = where + ' (source, source_id) >= (%s, %s)'
        values = values + [next_source, next_source_id]

    sql_query = f'''
        select
          company_id,
          country_of_interest,
          standardised_country,
          source,
          source_id,
          timestamp
        from coi_countries_of_interest
        {where}
        order by (source, source_id)
        limit {pagination_size} + 1
    '''
    df = execute_query(sql_query, data=values)
    if len(df) == pagination_size + 1:
        next_ = '{}{}'.format(request.host_url[:-1], request.path)
        next_ += '?'
        next_ += '&'.join(['country={}'.format(country) for country in countries])
        next_ += '&'.join(['source={}'.format(source) for source in sources])
        next_ += '&' if next_[-1] != '?' else ''
        next_ += 'orientation={}'.format(orientation)
        next_ += '&next-source={}&next-source-id={}'.format(
            df['source'].values[-1], df['source_id'].values[-1],
        )
        df = df[:-1]
    else:
        next_ = None
    web_dict = to_web_dict(df, orientation)
    web_dict['next'] = next_
    return flask_app.make_response(web_dict)


@api.route('/api/v1/get-company-export-countries')
@json_error
@response_orientation_decorator
@ac.authentication_required
@ac.authorization_required
def get_company_export_countries(orientation):
    pagination_size = flask_app.config['app']['pagination_size']
    next_source = request.args.get('next-source')
    next_source_id = request.args.get('next-source-id')
    company_ids = request.args.getlist('company-id')
    countries = request.args.getlist('country')
    sources = request.args.getlist('source')

    values = countries + sources + company_ids
    where = ''
    if len(countries) == 1:
        where = 'where export_country=%s'
    elif len(countries) > 1:
        where = (
            'where export_country in ('
            + ','.join('%s' for i in range(len(countries)))
            + ')'
        )
    if len(company_ids) == 1:
        where = where + ' and' if where != '' else 'where'
        where = where + ' company_id=%s'
    elif len(company_ids) > 1:
        where = where + ' and' if where != '' else 'where'
        where = (
            where
            + ' company_id in ('
            + ','.join(['%s' for i in range(len(company_ids))])
            + ')'
        )
    if len(sources) == 1:
        where = where + ' and' if where != '' else 'where'
        where = where + ' source=%s'
    elif len(sources) > 1:
        where = where + ' and' if where != '' else 'where'
        where = (
            where + ' source in (' + ','.join(['%s' for i in range(len(sources))]) + ')'
        )
    if next_source is not None and next_source_id is not None:
        where = where + ' and' if where != '' else 'where'
        where = where + ' (source, source_id) >= (%s, %s)'
        values = values + [next_source, next_source_id]

    sql_query = f'''
        select
          company_id,
          export_country,
          standardised_country,
          source,
          source_id,
          timestamp
        from coi_export_countries
        {where}
        order by (source, source_id)
        limit {pagination_size} + 1
    '''
    df = execute_query(sql_query, data=values)
    if len(df) == pagination_size + 1:
        next_ = '{}{}'.format(request.host_url[:-1], request.path)
        next_ += '?'
        next_ += '&'.join(['country={}'.format(country) for country in countries])
        next_ += '&'.join(['source={}'.format(source) for source in sources])
        next_ += '&' if next_[-1] != '?' else ''
        next_ += 'orientation={}'.format(orientation)
        next_ += '&next-source={}&next-source-id={}'.format(
            df['source'].values[-1], df['source_id'].values[-1],
        )
        df = df[:-1]
    else:
        next_ = None
    web_dict = to_web_dict(df, orientation)
    web_dict['next'] = next_
    return flask_app.make_response(web_dict)


@api.route('/api/v1/get-company-sectors-of-interest')
@json_error
@response_orientation_decorator
@ac.authentication_required
@ac.authorization_required
def get_company_sectors_of_interest(orientation):
    pagination_size = flask_app.config['app']['pagination_size']
    next_source = request.args.get('next-source')
    next_source_id = request.args.get('next-source-id')
    company_ids = request.args.getlist('company-id')
    sectors = request.args.getlist('sector')
    sources = request.args.getlist('source')

    values = sectors + sources + company_ids
    where = ''
    if len(sectors) == 1:
        where = 'where sector_of_interest=%s'
    elif len(sectors) > 1:
        where = (
            'where sector_of_interest in ('
            + ','.join('%s' for i in range(len(sectors)))
            + ')'
        )
    if len(company_ids) == 1:
        where = where + ' and' if where != '' else 'where'
        where = where + ' company_id=%s'
    elif len(company_ids) > 1:
        where = where + ' and' if where != '' else 'where'
        where = (
            where
            + ' company_id in ('
            + ','.join(['%s' for i in range(len(company_ids))])
            + ')'
        )
    if len(sources) == 1:
        where = where + ' and' if where != '' else 'where'
        where = where + ' source=%s'
    elif len(sources) > 1:
        where = where + ' and' if where != '' else 'where'
        where = (
            where + ' source in (' + ','.join(['%s' for i in range(len(sectors))]) + ')'
        )
    if next_source is not None and next_source_id is not None:
        where = where + ' and' if where != '' else 'where'
        where = where + ' (source, source_id) >= (%s, %s)'
        values = values + [next_source, next_source_id]

    sql_query = f'''
        select
          company_id,
          sector_of_interest,
          source,
          source_id,
          timestamp
        from coi_sectors_of_interest
        {where}
        order by (source, source_id)
        limit {pagination_size} + 1
    '''
    df = execute_query(sql_query, data=values)

    if len(df) == pagination_size + 1:
        next_ = '{}{}'.format(request.host_url[:-1], request.path)
        next_ += '?'
        next_ += '&'.join(['sector={}'.format(sector) for sector in sectors])
        next_ += '&'.join(['source={}'.format(source) for source in sources])
        next_ += '&' if next_[-1] != '?' else ''
        next_ += 'orientation={}'.format(orientation)
        next_ += '&next-source={}&next-source-id={}'.format(
            df['source'].values[-1], df['source_id'].values[-1],
        )
        df = df[:-1]
    else:
        next_ = None
    web_dict = to_web_dict(df, orientation)
    web_dict['next'] = next_
    return flask_app.make_response(web_dict)


@api.route('/')
@login_required
def get_index():
    last_updated = None
    if table_exists('etl_runs'):
        sql = 'select max(timestamp) from etl_runs'
        df = execute_query(sql)
        last_updated = pd.to_datetime(df.values[0][0])
    if last_updated is None:
        last_updated = 'Database not yet initialised'
    else:
        last_updated = last_updated.strftime('%Y-%m-%d %H:%M:%S')
    return render_template('index.html', last_updated=last_updated)


@api.route('/api/v1/populate-database')
@json_error
@ac.authentication_required
@ac.authorization_required
def populate_database():
    drop_table = 'drop-table' in request.args
    force_update = 'force-update' in request.args
    sql = (
        'create table if not exists etl_status '
        '(status varchar(100), timestamp timestamp)'
    )
    execute_statement(sql)
    sql = 'select * from etl_status'

    df = execute_query(sql)
    if force_update is True or len(df) == 0 or df['status'].values[0] == 'SUCCESS':
        populate_database_task.delay(drop_table)
        sql = 'delete from etl_status'
        execute_statement(sql)
        sql = '''insert into etl_status values (%s, %s)'''
        execute_statement(sql, data=['RUNNING', datetime.datetime.now()])
        return flask_app.make_response(
            {'status': 200, 'message': 'started populate_database task'}
        )
    else:
        timestamp = df['timestamp'].values[0]
        response = {
            'status': 200,
            'message': f"populate_database task already running since: {timestamp}",
        }
        return flask_app.make_response(response)


@api.route('/healthcheck/', methods=["GET"])
def healthcheck():
    return jsonify({"status": "OK"})
